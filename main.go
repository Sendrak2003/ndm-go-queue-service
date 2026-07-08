package main

import (
	"log"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"
)

type waiter struct {
	ch chan string
}

type Queue struct {
	mu      sync.Mutex
	items   []string
	waiters []waiter
}

func (q *Queue) Push(msg string) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.waiters) > 0 {
		w := q.waiters[0]
		q.waiters = q.waiters[1:]
		w.ch <- msg
		return
	}
	q.items = append(q.items, msg)
}

func (q *Queue) Pop() (string, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.items) == 0 {
		return "", false
	}
	msg := q.items[0]
	q.items = q.items[1:]
	return msg, true
}

func (q *Queue) PopWait(timeout time.Duration) (string, bool) {
	q.mu.Lock()
	if len(q.items) > 0 {
		msg := q.items[0]
		q.items = q.items[1:]
		q.mu.Unlock()
		return msg, true
	}
	ch := make(chan string, 1)
	q.waiters = append(q.waiters, waiter{ch: ch})
	q.mu.Unlock()

	select {
	case msg := <-ch:
		return msg, true
	case <-time.After(timeout):
		q.mu.Lock()
		for i, w := range q.waiters {
			if w.ch == ch {
				q.waiters = append(q.waiters[:i], q.waiters[i+1:]...)
				q.mu.Unlock()
				return "", false
			}
		}
		q.mu.Unlock()
		// waiter уже был удалён Push()-ем: сообщение точно доставлено в ch
		return <-ch, true
	}
}

var (
	queuesMu sync.Mutex
	queues   = make(map[string]*Queue)
)

func getQueue(name string) *Queue {
	queuesMu.Lock()
	defer queuesMu.Unlock()

	q, ok := queues[name]
	if !ok {
		q = &Queue{}
		queues[name] = q
	}
	return q
}

func putHandler(w http.ResponseWriter, r *http.Request) {
	val := r.URL.Query().Get("v")
	if val == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	getQueue(r.URL.Path[1:]).Push(val)
	w.WriteHeader(http.StatusOK)
}

func getHandler(w http.ResponseWriter, r *http.Request) {
	q := getQueue(r.URL.Path[1:])
	timeoutStr := r.URL.Query().Get("timeout")

	if timeoutStr == "" {
		if msg, ok := q.Pop(); ok {
			w.Write([]byte(msg))
			return
		}
		w.WriteHeader(http.StatusNotFound)
		return
	}

	t, err := strconv.Atoi(timeoutStr)
	if err != nil || t < 0 {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if msg, ok := q.PopWait(time.Duration(t) * time.Second); ok {
		w.Write([]byte(msg))
		return
	}
	w.WriteHeader(http.StatusNotFound)
}

func main() {
	port := "8080"
	if len(os.Args) > 1 {
		port = os.Args[1]
	}

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		switch r.Method {
		case http.MethodPut:
			putHandler(w, r)
		case http.MethodGet:
			getHandler(w, r)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})

	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatal(err)
	}
}
