package main

import (
	"bytes"
	_ "bytes"
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	kafka "github.com/segmentio/kafka-go"
)

const (
	groupID      = "events"
	topicMovie   = "movie-events"
	topicUser    = "user-events"
	topicPayment = "payment-events"
	defaultPort  = "8082"
	headerJSON   = "application/json"
	timeRFC3339  = time.RFC3339
	idTimeFormat = "20060102T150405Z"
)

type Event struct {
	ID        string                 `json:"id"`
	Type      string                 `json:"type"`      // movie | user | payment
	Timestamp string                 `json:"timestamp"` // RFC3339
	Payload   map[string]interface{} `json:"payload"`
}

type EventResponse struct {
	Status    string `json:"status"`    // "success"
	Partition int    `json:"partition"` // MVP-заглушка
	Offset    int64  `json:"offset"`    // MVP-заглушка
	Event     Event  `json:"event"`
}

type ErrorResponse struct {
	Error string `json:"error"`
}

type MovieEvent struct {
	MovieID     int      `json:"movie_id"`
	Title       string   `json:"title"`
	Action      string   `json:"action"`
	UserID      *int     `json:"user_id,omitempty"`
	Rating      *float64 `json:"rating,omitempty"`
	Genres      []string `json:"genres,omitempty"`
	Description *string  `json:"description,omitempty"`
}

type UserEvent struct {
	UserID    int       `json:"user_id"`
	Username  *string   `json:"username,omitempty"`
	Email     *string   `json:"email,omitempty"`
	Action    string    `json:"action"`
	Timestamp time.Time `json:"timestamp"`
}

type PaymentEvent struct {
	PaymentID int       `json:"payment_id"`
	UserID    int       `json:"user_id"`
	Amount    float64   `json:"amount"`
	Status    string    `json:"status"`
	Timestamp time.Time `json:"timestamp"`
	Method    *string   `json:"method_type,omitempty"`
}

type KafkaHub struct {
	brokers []string
	writers map[string]*kafka.Writer
	readers map[string]*kafka.Reader
	closing chan struct{}
}

// truncate безопасно обрезает большие тела для логов
func truncate(b []byte, max int) string {
	if len(b) <= max {
		return string(b)
	}
	return string(b[:max]) + "...(truncated)"
}

// RequestLogger логирует метод, путь, query, базовые заголовки и тело запроса.
// Корректно "возвращает" тело обратно в c.Request.Body, чтобы хендлеры могли его читать.
func RequestLogger() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()

		// Считываем и восстанавливаем тело
		body, _ := io.ReadAll(c.Request.Body)
		c.Request.Body = io.NopCloser(bytes.NewBuffer(body))

		// Соберём информацию о запросе
		method := c.Request.Method
		path := c.Request.URL.Path
		query := c.Request.URL.RawQuery
		if query != "" {
			path = path + "?" + query
		}

		ct := c.GetHeader("Content-Type")
		ua := c.GetHeader("User-Agent")
		xreq := c.GetHeader("X-Request-Id")

		log.Printf(
			"[http] incoming %s %s from=%s content-type=%q ua=%q x-request-id=%q body=%s",
			method, path, c.ClientIP(), ct, ua, xreq, truncate(body, 4096),
		)

		c.Next()

		log.Printf("[http] completed %s %s status=%d dur=%s",
			method, path, c.Writer.Status(), time.Since(start))
	}
}

func NewKafkaHub(brokersCSV string, topics []string) *KafkaHub {
	brokers := splitAndTrim(brokersCSV)
	h := &KafkaHub{
		brokers: brokers,
		writers: make(map[string]*kafka.Writer, len(topics)),
		readers: make(map[string]*kafka.Reader, len(topics)),
		closing: make(chan struct{}),
	}
	for _, t := range topics {
		h.writers[t] = &kafka.Writer{
			Addr:         kafka.TCP(brokers...),
			Topic:        t,
			Balancer:     &kafka.LeastBytes{},
			RequiredAcks: kafka.RequireAll,
			Async:        false,
		}
		h.readers[t] = kafka.NewReader(kafka.ReaderConfig{
			Brokers:     brokers,
			GroupID:     groupID,
			Topic:       t,
			MinBytes:    1,
			MaxBytes:    10e6,
			StartOffset: kafka.FirstOffset,
		})
	}
	return h
}

func (h *KafkaHub) Publish(ctx context.Context, topic string, body []byte, key string) (int, int64, error) {
	w := h.writers[topic]
	msg := kafka.Message{
		Key:   []byte(key),
		Value: body, // отправляем ровно то, что пришло в body
		Headers: []kafka.Header{
			{Key: "content-type", Value: []byte(headerJSON)},
			{Key: "x-produced-at", Value: []byte(time.Now().UTC().Format(timeRFC3339))},
		},
	}
	if err := w.WriteMessages(ctx, msg); err != nil {
		return 0, 0, err
	}
	return 0, 0, nil
}

func (h *KafkaHub) StartConsumers(ctx context.Context) {
	for topic, r := range h.readers {
		t := topic
		reader := r
		go func() {
			log.Printf("[kafka] consumer started topic=%s group=%s", t, groupID)
			for {
				m, err := reader.ReadMessage(ctx)
				if err != nil {
					select {
					case <-h.closing:
						log.Printf("[kafka] consumer stopped topic=%s", t)
						return
					default:
						log.Printf("[kafka] read error topic=%s: %v", t, err)
						time.Sleep(500 * time.Millisecond)
						continue
					}
				}
				// Печатаем тело как есть; если это JSON — покажем prettified
				var generic map[string]interface{}
				if err := json.Unmarshal(m.Value, &generic); err == nil {
					j, _ := json.Marshal(generic)
					log.Printf("[kafka] consumed topic=%s partition=%d offset=%d body=%s",
						t, m.Partition, m.Offset, string(j))
				} else {
					log.Printf("[kafka] consumed raw topic=%s partition=%d offset=%d value=%s",
						t, m.Partition, m.Offset, string(m.Value))
				}
			}
		}()
	}
}

func (h *KafkaHub) Close() {
	close(h.closing)
	for t, r := range h.readers {
		if err := r.Close(); err != nil {
			log.Printf("[kafka] reader close error topic=%s: %v", t, err)
		}
	}
	for t, w := range h.writers {
		if err := w.Close(); err != nil {
			log.Printf("[kafka] writer close error topic=%s: %v", t, err)
		}
	}
}

type API struct {
	hub *KafkaHub
}

func NewAPI(h *KafkaHub) *API { return &API{hub: h} }

func (a *API) Router() *gin.Engine {
	r := gin.Default()
	r.Use(RequestLogger())
	r.GET("/api/events/health", a.health)
	r.POST("/api/events/movie", a.createMovieEvent)
	r.POST("/api/events/user", a.createUserEvent)
	r.POST("/api/events/payment", a.createPaymentEvent)
	return r
}

func (a *API) health(c *gin.Context) { c.JSON(http.StatusOK, gin.H{"status": true}) }

func (a *API) createMovieEvent(c *gin.Context) {
	raw, err := io.ReadAll(c.Request.Body)
	if err != nil || len(raw) == 0 {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: "empty body"})
		return
	}
	var in MovieEvent
	if err := json.Unmarshal(raw, &in); err != nil || in.MovieID == 0 || in.Title == "" || in.Action == "" {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: "invalid movie event payload"})
		return
	}
	ev := Event{
		ID:        "movie-" + strconv.Itoa(in.MovieID) + "-" + in.Action + "-" + time.Now().UTC().Format(idTimeFormat),
		Type:      "movie",
		Timestamp: time.Now().UTC().Format(timeRFC3339),
		Payload:   toMap(raw),
	}
	resp := EventResponse{Status: "success", Partition: 0, Offset: 0, Event: ev}

	log.Printf("[api] publishing topic=%s key=%s body=%s", topicMovie, "movie", truncate(raw, 4096))

	if _, _, err := a.hub.Publish(c.Request.Context(), topicMovie, raw, "movie"); err != nil {
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: err.Error()})
		return
	}
	c.JSON(http.StatusCreated, resp)
}

func (a *API) createUserEvent(c *gin.Context) {
	raw, err := io.ReadAll(c.Request.Body)
	if err != nil || len(raw) == 0 {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: "empty body"})
		return
	}
	var in UserEvent
	if err := json.Unmarshal(raw, &in); err != nil || in.UserID == 0 || in.Action == "" || in.Timestamp.IsZero() {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: "invalid user event payload"})
		return
	}
	ev := Event{
		ID:        "user-" + strconv.Itoa(in.UserID) + "-" + in.Action + "-" + time.Now().UTC().Format(idTimeFormat),
		Type:      "user",
		Timestamp: in.Timestamp.UTC().Format(timeRFC3339),
		Payload:   toMap(raw),
	}
	resp := EventResponse{Status: "success", Partition: 0, Offset: 0, Event: ev}

	log.Printf("[api] publishing topic=%s key=%s body=%s", topicUser, "user", truncate(raw, 4096))

	if _, _, err := a.hub.Publish(c.Request.Context(), topicUser, raw, "user"); err != nil {
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: err.Error()})
		return
	}
	c.JSON(http.StatusCreated, resp)
}

func (a *API) createPaymentEvent(c *gin.Context) {
	raw, err := io.ReadAll(c.Request.Body)
	if err != nil || len(raw) == 0 {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: "empty body"})
		return
	}
	var in PaymentEvent
	if err := json.Unmarshal(raw, &in); err != nil || in.PaymentID == 0 || in.UserID == 0 || in.Status == "" || in.Timestamp.IsZero() {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: "invalid payment event payload"})
		return
	}
	ev := Event{
		ID:        "payment-" + strconv.Itoa(in.PaymentID) + "-" + in.Status + "-" + time.Now().UTC().Format(idTimeFormat),
		Type:      "payment",
		Timestamp: in.Timestamp.UTC().Format(timeRFC3339),
		Payload:   toMap(raw),
	}
	resp := EventResponse{Status: "success", Partition: 0, Offset: 0, Event: ev}

	log.Printf("[api] publishing topic=%s key=%s body=%s", topicPayment, "payment", truncate(raw, 4096))

	if _, _, err := a.hub.Publish(c.Request.Context(), topicPayment, raw, "payment"); err != nil {
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: err.Error()})
		return
	}
	c.JSON(http.StatusCreated, resp)
}

// ---- main / infra ----

func main() {
	brokers := getenv("KAFKA_BROKERS", "localhost:9092")
	port := getenv("PORT", defaultPort)

	topics := []string{topicMovie, topicUser, topicPayment}
	hub := NewKafkaHub(brokers, topics)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hub.StartConsumers(ctx)

	api := NewAPI(hub)
	srv := &http.Server{Addr: ":" + port, Handler: api.Router()}

	go func() {
		log.Printf("[http] listening on :%s | brokers=%s | group=%s | topics=%v",
			port, brokers, groupID, topics)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("[http] server error: %v", err)
		}
	}()

	// graceful shutdown
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	log.Printf("[main] shutting down...")
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelShutdown()
	_ = srv.Shutdown(shutdownCtx)
	hub.Close()
	log.Printf("[main] bye")
}

func getenv(k, def string) string {
	v := strings.TrimSpace(os.Getenv(k))
	if v == "" {
		return def
	}
	return v
}

func splitAndTrim(csv string) []string {
	parts := strings.Split(csv, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func toMap(raw []byte) map[string]interface{} {
	var m map[string]interface{}
	_ = json.Unmarshal(raw, &m)
	return m
}
