package retranslator

import (
	"errors"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/ozonmp/cnm-serial-api/internal/mocks"
	"github.com/ozonmp/cnm-serial-api/internal/model"
)

func TestStart(t *testing.T) {

	ctrl := gomock.NewController(t)
	repo := mocks.NewMockEventRepo(ctrl)
	sender := mocks.NewMockEventSender(ctrl)

	cfg := Config{
		ChannelSize:    512,
		ConsumerCount:  2,
		ConsumeSize:    10,
		ConsumeTimeout: 10 * time.Second,
		ProducerCount:  2,
		WorkerCount:    2,
		Repo:           repo,
		Sender:         sender,
	}

	//test run (blank)
	t.Run("test_run", func(t *testing.T) {
		retranslator := NewRetranslator(cfg)
		retranslator.Start()
		retranslator.Close()
	})

	//test run with db connection error
	t.Run("error_db_conn", func(t *testing.T) {
		repo.EXPECT().Lock(gomock.Any()).AnyTimes().Return(nil, errors.New("db connection error"))
		sender.EXPECT().Send(gomock.Any()).Times(0)
		repo.EXPECT().Unlock(gomock.Any()).Times(0)
		repo.EXPECT().Remove(gomock.Any()).Times(0)

		retranslator := NewRetranslator(cfg)
		retranslator.Start()
		time.Sleep(time.Millisecond)
		retranslator.Close()
	})

	//test run with empty events
	t.Run("empty_events", func(t *testing.T) {
		events := []model.SerialEvent{}
		repo.EXPECT().Lock(gomock.Any()).AnyTimes().Return(events, nil)
		sender.EXPECT().Send(gomock.Any()).Times(0)
		repo.EXPECT().Unlock(gomock.Any()).Times(0)
		repo.EXPECT().Remove(gomock.Any()).Times(0)

		retranslator := NewRetranslator(cfg)
		retranslator.Start()
		time.Sleep(time.Millisecond)
		retranslator.Close()
	})

	// send test instance
	t.Run("events", func(t *testing.T) {
		events := []model.SerialEvent{
			{
				ID:     uint64(13),
				Type:   model.Created,
				Status: model.Processed,
				Entity: &model.Serial{
					ID:    1,
					Title: "Serial one",
					Year:  2001,
				},
			},
		}

		repo.EXPECT().Lock(cfg.ConsumeSize).AnyTimes().Return(events, nil)
		sender.EXPECT().Send(gomock.Any()).AnyTimes()
		repo.EXPECT().Unlock(gomock.Any()).AnyTimes()
		repo.EXPECT().Remove(gomock.Any()).AnyTimes()

		retranslator := NewRetranslator(cfg)
		retranslator.Start()
		time.Sleep(time.Millisecond * 1000)
		retranslator.Close()
	})

}
