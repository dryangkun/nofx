package trader

//-----------dev_my---------------

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"github.com/gorilla/websocket"
	"net/http"
	"nofx/logger"
	"strconv"
	"sync/atomic"
	"time"
)

var okxWsMessagePing = []byte("ping")

var okxWsMessagePong = []byte("pong")

type okxWsCommonMessage struct {
	Event string `json:"event"`
	Code  string `json:"code"`
	Msg   string `json:"msg"`
	Arg   struct {
		Channel string `json:"channel"`
	} `json:"arg"`
}

type okxWsClientConn struct {
	*websocket.Conn // c.Close() can stop read-goroutine
	errChannel      chan error
}

func okxWsClientConnNew(c *websocket.Conn) *okxWsClientConn {
	return &okxWsClientConn{
		Conn:       c,
		errChannel: make(chan error, 1),
	}
}

func (m *okxWsClientConn) send(msg []byte) {
	if err := m.WriteMessage(websocket.TextMessage, msg); err != nil {
		m.sendError(err)
	}
}

func (m *okxWsClientConn) sendError(err error) {
	select {
	case m.errChannel <- err:
	}
}

type okxWsClient struct {
	trader       *OKXTrader
	url          string
	timeout      time.Duration
	dialer       *websocket.Dialer
	shutdownFlag atomic.Bool
	shutdownDone chan struct{}
	onLogin      func(client *okxWsClient, conn *okxWsClientConn)
	onMessage    func(client *okxWsClient, b []byte, d *okxWsCommonMessage)
}

func okxWsClientNew(trader *OKXTrader) *okxWsClient {
	timeout := 15 * time.Second
	return &okxWsClient{
		trader:  trader,
		url:     "wss://ws.okx.com:8443/ws/v5/private",
		timeout: timeout,
		dialer: &websocket.Dialer{
			Proxy:            http.ProxyFromEnvironment,
			HandshakeTimeout: timeout,
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		},
		shutdownDone: make(chan struct{}),
	}
}

func (m *okxWsClient) connect() (error, *websocket.Conn, *http.Response) {
	start := time.Now()
	c, resp, err := m.dialer.Dial(m.url, nil)
	if err != nil {
		logger.Errorf("⚠️ OKX websocket client(%s) connect failed, %v", m.trader.apiKey, err)
		if elapsed := time.Since(start); elapsed < m.timeout {
			time.Sleep(m.timeout - elapsed)
		}
		return err, nil, nil
	}
	logger.Infof("  ✓ OKX websocket client(%s) connect successfully", m.trader.apiKey)
	return nil, c, resp
}

func (m *okxWsClient) goMonit() {
	var err error
	var c *websocket.Conn
	var conn *okxWsClientConn
	for {
		if err, c, _ = m.connect(); err != nil {
			goto end
		}
		conn = okxWsClientConnNew(c)
		go m.goReadStart(conn)
		//login
		{
			timestamp := strconv.FormatInt(time.Now().Unix(), 10)
			sign := m.trader.sign(timestamp, "GET", "/users/self/verify", "")
			b, _ := json.Marshal(map[string]interface{}{
				"op": "login",
				"args": []map[string]string{
					{
						"apiKey":     m.trader.apiKey,
						"passphrase": m.trader.passphrase,
						"timestamp":  timestamp,
						"sign":       sign,
					},
				},
			})
			if err = conn.WriteMessage(websocket.TextMessage, b); err != nil {
				logger.Infof("⚠️ OKX websocket client(%s) login write error, %v", m.trader.apiKey, err)
				goto end
			}
		}
	loop:
		for {
			select {
			case <-m.shutdownDone:
				break loop
			case err = <-conn.errChannel:
				logger.Infof("⚠️ OKX websocket client(%s) error, %v", m.trader.apiKey, err)
				break loop
			}
		}

	end:
		if c != nil {
			_ = c.Close()
		}
		if m.shutdownFlag.Load() {
			break
		}
	}
	logger.Infof("OKX websocket client(%s) shutdown", m.trader.apiKey)
}

func (m *okxWsClient) goReadStart(conn *okxWsClientConn) {
	var pingTimer *time.Timer
	var pongTimerRef atomic.Pointer[time.Timer]
	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			if pongTimer := pongTimerRef.Load(); pongTimer != nil {
				pongTimer.Stop()
			}
			if pingTimer != nil {
				pingTimer.Stop()
			}
			conn.sendError(err)
			return
		}
		if bytes.Compare(msg, okxWsMessagePong) != 0 {
			var data *okxWsCommonMessage
			if err = json.Unmarshal(msg, &data); err != nil {
				continue
			}
			switch data.Event {
			case "error":
				logger.Infof("⚠️ OKX websocket client(%s) receive error, %s", m.trader.apiKey, msg)
			case "login":
				m.onLogin(m, conn)
			default:
				m.onMessage(m, msg, data)
			}
		}
		if pongTimer := pongTimerRef.Swap(nil); pongTimer != nil {
			pongTimer.Stop()
		}
		if pingTimer != nil {
			pingTimer.Reset(m.timeout)
			continue
		}
		pingTimer = time.AfterFunc(m.timeout, func() {
			conn.send(okxWsMessagePing)
			pongTimerRef.Store(time.AfterFunc(m.timeout, func() {
				logger.Infof("⚠️ OKX websocket client(%s) no message timeout %s", m.trader.apiKey, m.timeout)
				conn.sendError(fmt.Errorf("no message timeout"))
				return
			}))
		})
	}
}

func (m *okxWsClient) shutdown() {
	logger.Info("OKX websocket client(%s) start to shutdown", m.trader.apiKey)
	if m.shutdownFlag.CompareAndSwap(false, true) {
		close(m.shutdownDone)
	}
}

func (m *okxWsClient) start() {
	go m.goMonit()
}

//-----------dev_my---------------
