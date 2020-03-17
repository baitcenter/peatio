# frozen_string_literal: true

module Workers
  module AMQP
    class InfluxWriter < Base
      ZERO = '0.0'.to_d

      def initialize(dry_run = false)
        return if dry_run

        Thread.new do
          loop do
            tickers = {}
            Market.enabled.each do |market|
              ticker = Trade.market_ticker_from_influx(market.id)
              tickers[market.id] = format(ticker, market.id)
            end
            Rails.logger.info { "Publish tickers: #{tickers}" }
            ::AMQP::Queue.enqueue_event('public', 'global', 'tickers', tickers)
            sleep 2
          end
        end
      end

      def process(payload, metadata, _delivery_info)
        case metadata[:headers]['type']
        when 'local'
          trade = Trade.new payload
          trade.write_to_influx
        when 'upstream'
          trade = Trade.new payload.merge(total: payload['price'].to_d * payload['amount'].to_d)
          trade.write_to_influx
        end
      end

      def format(ticker, market_id)
        if ticker.blank?
          ticker = { min: ZERO, max: ZERO, last: ZERO, first: ZERO, volume: ZERO, amount: ZERO, vwap: ZERO }
          last_trade = Trade.public_from_influx(market_id, 1).first
          ticker[:last] = last_trade[:price] if last_trade.present?
        end

        {
          at: Time.now.to_i,
          avg_price: ticker[:vwap],
          high: ticker[:max],
          last: ticker[:last],
          low: ticker[:min],
          open: ticker[:first],
          price_change_percent: change_ratio(ticker[:first], ticker[:last]),
          volume: ticker[:volume],
          amount: ticker[:amount]
        }
      end

      def change_ratio(open, last)
        percent = open.zero? ? 0 : (last - open) / open * 100

        # Prepend sign. Show two digits after the decimal point. Append '%'.
        "#{'%+.2f' % percent}%"
      end
    end
  end
end
