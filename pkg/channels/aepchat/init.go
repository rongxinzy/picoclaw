package aepchat

import (
	"github.com/sipeed/picoclaw/pkg/bus"
	"github.com/sipeed/picoclaw/pkg/channels"
	"github.com/sipeed/picoclaw/pkg/config"
)

func init() {
	channels.RegisterFactory(
		config.ChannelAEPChat,
		func(channelName, channelType string, cfg *config.Config, b *bus.MessageBus) (channels.Channel, error) {
			bc := cfg.Channels[channelName]
			decoded, err := bc.GetDecoded()
			if err != nil {
				return nil, err
			}
			settings, ok := decoded.(*config.AEPChatSettings)
			if !ok {
				return nil, channels.ErrSendFailed
			}
			ch, err := New(channelName, bc, settings, cfg, b)
			if err != nil {
				return nil, err
			}
			if channelName != config.ChannelAEPChat {
				ch.SetName(channelName)
			}
			return ch, nil
		},
	)
}
