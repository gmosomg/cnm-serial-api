package producer

import (
	"log"
	"sync"
	"time"

	"github.com/ozonmp/cnm-serial-api/internal/app/repo"
	"github.com/ozonmp/cnm-serial-api/internal/app/sender"
	"github.com/ozonmp/cnm-serial-api/internal/model"

	"github.com/gammazero/workerpool"
)

type Producer interface {
	Start()
	Close()
}

type producer struct {
	n uint64
	// NOT USED timeout time.Duration

	sender sender.EventSender
	events <-chan model.SerialEvent
	repo   repo.EventRepo

	workerPool *workerpool.WorkerPool

	wg   *sync.WaitGroup
	done chan bool
}

// todo for students: add repo
func NewKafkaProducer(
	n uint64,
	sender sender.EventSender,
	repo repo.EventRepo,
	events <-chan model.SerialEvent,
	workerPool *workerpool.WorkerPool,
) Producer {

	wg := &sync.WaitGroup{}
	done := make(chan bool)

	return &producer{
		n:          n,
		sender:     sender,
		repo:       repo,
		events:     events,
		workerPool: workerPool,
		wg:         wg,
		done:       done,
	}
}

func (p *producer) Start() {
	for i := uint64(0); i < p.n; i++ {
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()

			runCleaner := make(chan int, 3)
			runUpdater := make(chan int, 3)

			cleanerBuf := NewMtxBuffer()
			updaterBuf := NewMtxBuffer()

			bufTimeout := 1 * time.Second
			ticker := time.NewTicker(bufTimeout)

			for {
				select {

				case event := <-p.events:
					if err := p.sender.Send(&event); err != nil {
						if cleanerBuf.Put(event.ID) > 100 {
							runCleaner <- 1
						}
					} else {
						if updaterBuf.Put(event.ID) > 100 {
							runUpdater <- 1
						}
					}

				case <-runCleaner:
					p.workerPool.Submit(func() {
						// ... cleaner (refer to diagram)
						// remove db records
						cleanIDs := cleanerBuf.Get()
						if len(cleanIDs) < 1 {
							return
						}

						if err := p.repo.Remove(cleanIDs); err == nil {
							return
						} else {
							log.Println("Err: repo.Remove failed:", err)
							// Perfect world situation - No errors
							// TODO process errs: break or retry
							// put ids to updaters for now:
							// updaterBuf.Add(cleanIDs)
							// runUpdater <- 1
						}
					})

				case <-runUpdater:
					p.workerPool.Submit(func() {
						// ... updater (refer to diagram)
						// update db records
						updateIDs := updaterBuf.Get()
						if len(updateIDs) < 1 {
							return
						}

						if err := p.repo.Unlock(updateIDs); err == nil {
							return
						} else {
							log.Println("Err: repo.Unlock failed:", err)
							// Perfect world situation - No errors
							// TODO process errs: break or retry
						}
					})

				case <-ticker.C:
					// process all buffers by timeout
					runCleaner <- 1
					runUpdater <- 1

				case <-p.done:
					// stop event processing ?
					// process all buffers ?
					runCleaner <- 1
					close(runCleaner)
					runUpdater <- 1
					close(runUpdater)
					return
				}
			}

		}()
	}
}

func (p *producer) Close() {
	close(p.done)
	p.wg.Wait()
}

type mtxBuffer struct {
	mtx    sync.Mutex
	buffer []uint64
}

func NewMtxBuffer() *mtxBuffer {
	buf := []uint64{}
	return &mtxBuffer{
		mtx:    sync.Mutex{},
		buffer: buf,
	}
}

func (m *mtxBuffer) Put(ids ...uint64) int {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	m.buffer = append(m.buffer, ids...)
	return len(m.buffer)
}

func (m *mtxBuffer) Add(ids []uint64) int {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	m.buffer = append(m.buffer, ids...)
	return len(m.buffer)
}

func (m *mtxBuffer) Get() []uint64 {
	m.mtx.Lock()
	defer func() {
		m.buffer = nil
		m.buffer = []uint64{}
		m.mtx.Unlock()
	}()
	return m.buffer
}

func (m *mtxBuffer) Len() int {
	return len(m.buffer)
}
