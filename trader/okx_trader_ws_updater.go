package trader

import (
	"encoding/json"
	"strconv"
	"sync"
	"time"
)

type okxSubscribeDataAccount struct {
	Data []struct {
		TotalEq string `json:"totalEq"`
		Details []struct {
			Ccy      string `json:"ccy"`
			Eq       string `json:"eq"`
			CashBal  string `json:"cashBal"`
			AvailBal string `json:"availBal"`
			UPL      string `json:"upl"`
		} `json:"details"`
	} `json:"data"`
}

type okxSubscribeDataPositionsV struct {
	MgnMode string `json:"mgnMode"`
	InstId  string `json:"instId"`
	PosSide string `json:"posSide"`
	Pos     string `json:"pos"`
	AvgPx   string `json:"avgPx"`
	MarkPx  string `json:"markPx"`
	Upl     string `json:"upl"`
	Lever   string `json:"lever"`
	LiqPx   string `json:"liqPx"`
	Margin  string `json:"margin"`
	CTime   string `json:"cTime"`
	UTime   string `json:"uTime"`
}

type okxSubscribeDataPositions struct {
	EventType string `json:"eventType"`
	LastPage  bool   `json:"lastPage"`
	Data      []*okxSubscribeDataPositionsV
}

type okxTraderWsUpdaterEventPositions struct {
	eventType string //snapshot / event_update
	data      []*okxSubscribeDataPositionsV
}

// okxTraderWsClient update OKXTrader.cachedBalance/OKXTrader.cachedPositions by websocket
type okxTraderWsUpdater struct {
	trader      *OKXTrader
	maxIdleTime time.Duration //client keep alive max time when idle
	ch          chan interface{}
	client      *okxWsClient
	lock        sync.RWMutex
	idleTimer   *time.Timer
	// use in client read-goroutine
	positionsData []*okxSubscribeDataPositionsV
}

func okxTraderWsUpdaterNew(trader *OKXTrader, maxIdleTime time.Duration) *okxTraderWsUpdater {
	updater := &okxTraderWsUpdater{
		trader:      trader,
		maxIdleTime: maxIdleTime,
		ch:          make(chan interface{}, 100),
	}
	go updater.goStart()
	return updater
}

func (m *okxTraderWsUpdater) goStart() {
	positions := make(map[string]map[string]interface{})
	for d := range m.ch {
		switch data := d.(type) {
		case map[string]interface{}:
			m.trader.balanceCacheMutex.Lock()
			m.trader.cachedBalance = data
			m.trader.balanceCacheTime = time.Now()
			m.trader.balanceCacheMutex.Unlock()

		case *okxTraderWsUpdaterEventPositions:
			if data.eventType == "snapshot" {
				positions = make(map[string]map[string]interface{})
			}
			for _, pos := range data.data {
				if pos.MgnMode != "cross" {
					continue
				}
				posAmt, _ := strconv.ParseFloat(pos.Pos, 64)
				if posAmt == 0 {
					delete(positions, pos.InstId)
					continue
				}
				entryPrice, _ := strconv.ParseFloat(pos.AvgPx, 64)
				markPrice, _ := strconv.ParseFloat(pos.MarkPx, 64)
				upl, _ := strconv.ParseFloat(pos.Upl, 64)
				leverage, _ := strconv.ParseFloat(pos.Lever, 64)
				liqPrice, _ := strconv.ParseFloat(pos.LiqPx, 64)
				symbol := m.trader.convertSymbolBack(pos.InstId)
				side := "long"
				if pos.PosSide == "short" {
					side = "short"
				}
				if posAmt < 0 {
					posAmt = -posAmt
				}
				cTime, _ := strconv.ParseInt(pos.CTime, 10, 64)
				uTime, _ := strconv.ParseInt(pos.UTime, 10, 64)
				positions[pos.InstId] = map[string]interface{}{
					"symbol":           symbol,
					"positionAmt":      posAmt,
					"entryPrice":       entryPrice,
					"markPrice":        markPrice,
					"unRealizedProfit": upl,
					"leverage":         leverage,
					"liquidationPrice": liqPrice,
					"side":             side,
					"createdTime":      cTime, // Position open time (ms)
					"updatedTime":      uTime, // Position last update time (ms)
				}
			}

			var result []map[string]interface{}
			for _, pos := range positions {
				pos1 := make(map[string]interface{}, len(pos))
				for k, v := range pos {
					pos1[k] = v
				}
				result = append(result, pos)
			}
			m.trader.positionsCacheMutex.Lock()
			m.trader.cachedPositions = result
			m.trader.positionsCacheTime = time.Now()
			m.trader.positionsCacheMutex.Unlock()

		}
	}
}

func (m *okxTraderWsUpdater) onLogin(client *okxWsClient, conn *okxWsClientConn) {
	m.positionsData = nil
	b, _ := json.Marshal(map[string]interface{}{
		"op": "subscribe",
		"args": []map[string]interface{}{
			{
				"channel":     "account",
				"extraParams": "{\"updateInterval\":\"1\"}",
			},
			{
				"channel":     "positions",
				"instType":    "SWAP",
				"extraParams": "{\"updateInterval\":\"2000\"}",
			},
		},
	})
	conn.send(b)
}

func (m *okxTraderWsUpdater) onMessage(client *okxWsClient, b []byte, d *okxWsCommonMessage) {
	if d.Event != "" {
		return
	}
	//subscribe data
	switch d.Arg.Channel {
	case "account":
		var data *okxSubscribeDataAccount
		if err := json.Unmarshal(b, &data); err != nil {
			return
		}
		if len(data.Data) == 0 {
			return
		}
		balance := data.Data[0]
		totalEq, _ := strconv.ParseFloat(balance.TotalEq, 64)
		var usdtAvail, usdtUPL float64
		var found = false
		for _, detail := range balance.Details {
			if detail.Ccy == "USDT" {
				usdtAvail, _ = strconv.ParseFloat(detail.AvailBal, 64)
				usdtUPL, _ = strconv.ParseFloat(detail.UPL, 64)
				found = true
				break
			}
		}
		if !found {
			return
		}
		m.ch <- map[string]interface{}{
			"totalWalletBalance":    totalEq,
			"availableBalance":      usdtAvail,
			"totalUnrealizedProfit": usdtUPL,
		}

	case "positions":
		var data *okxSubscribeDataPositions
		if err := json.Unmarshal(b, &data); err != nil {
			return
		}
		if data.EventType == "snapshot" {
			m.positionsData = append(m.positionsData, data.Data...)
			if data.LastPage {
				m.ch <- &okxTraderWsUpdaterEventPositions{data.EventType, m.positionsData}
				m.positionsData = nil
			}
		} else {
			m.ch <- &okxTraderWsUpdaterEventPositions{data.EventType, data.Data}
		}
	}
}

func (m *okxTraderWsUpdater) tryStart() {
	m.lock.RLock()
	if m.idleTimer != nil {
		m.idleTimer.Reset(m.maxIdleTime)
		m.lock.RUnlock()
		return
	}
	m.lock.RUnlock()

	if m.lock.TryLock() {
		if m.idleTimer == nil {
			m.idleTimer = time.AfterFunc(m.maxIdleTime, func() {
				m.lock.Lock()
				defer m.lock.Unlock()
				m.client.shutdown()
				m.client = nil
				m.idleTimer = nil
			})
			m.client = okxWsClientNew(m.trader)
			m.client.onLogin = m.onLogin
			m.client.onMessage = m.onMessage
			m.client.start()
		}
		m.lock.Unlock()
	}
}
