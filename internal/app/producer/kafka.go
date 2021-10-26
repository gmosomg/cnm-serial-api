package producer

import (
	"log"
	"sync"

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

			cleanerBuf := NewMtxBuffer()
			updaterBuf := NewMtxBuffer()

			for {
				select {

				case event := <-p.events:
					if err := p.sender.Send(&event); err != nil {
						p.workerPool.Submit(func() {
							// ... cleaner (refer to diagram)
							// remove db records
							cleanerBuf.Put(event.ID)
							cleanIDs := cleanerBuf.Get()

							if len(cleanIDs) < 1 {
								return
							}

							if err := p.repo.Remove(cleanIDs); err == nil {
								return
							}
							log.Println("Err: repo.Remove failed:", err)
							// TODO process errs: break or retry
							// put ids to updaters for now
							updaterBuf.Add(cleanIDs)
						})
					} else {
						p.workerPool.Submit(func() {
							// ... updater (refer to diagram)
							// update db records
							updaterBuf.Put(event.ID)
							updateIDs := updaterBuf.Get()

							if len(updateIDs) < 1 {
								return
							}

							if err := p.repo.Unlock(updateIDs); err == nil {
								return
							}
							log.Println("Err: repo.Unlock failed:", err)
							// TODO process errs: break or retry
							// return ids to updaters for now
							updaterBuf.Add(updateIDs)
						})
					}

				case <-p.done:
					return
				}
			}

		}()
	}
}

func (p *producer) Close() {
	// p.workerPool.StopWait()
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

func (m *mtxBuffer) Put(ids ...uint64) {
	m.mtx.Lock()
	m.buffer = append(m.buffer, ids...)
	m.mtx.Unlock()
}

func (m *mtxBuffer) Add(ids []uint64) {
	m.mtx.Lock()
	m.buffer = append(m.buffer, ids...)
	m.mtx.Unlock()
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
