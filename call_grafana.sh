logcli query --addr=https://grafana-loki-gateway.internal.ablycorp.com \
  --org-id=fake \
  \
  --from=2025-01-01T00:00:00Z \
  --to=2025-01-13T13:50:00Z \
  \
  --parallel-duration=30m \
  --parallel-max-workers=100 \
  \
  --part-path-prefix=/tmp/logcli \
  --overwrite-completed-parts \
  --merge-parts \
  \
  --output=jsonl \
  \
  '{service="seller-api"} |= `vendor_seller_update_goods` | json | ably_market_sno =~ `34466|1158|6033|5479|10007|11012|3326|11487|4897|9738|10910|14565|14175|5566|14557|9172|8813|14109|9169|7267|18675|17119|10321|5916|19734|3707|10181|22419|5319|21655|18454|23880|24745|14168|25109|25306|26186|26229|25335|26235|26238|13296|26234|26972|5019|28434|10595|29514|27001|28597|29866|10618|29791|5376|30201|29516|31173|2048|13037|31239|1693|31596|30376|25914|32368|18813|30861|33878|33617|33696|34904|1348|14207|3547|32595|10745|13963|35159|1485|23164|33549|35831|31701|36160|7914|35433|28613|34130|1510|36889|8422|17840|10173|10368|2237|17790|30785|11897|36733` | line_format `{{.ably_market_sno}},{{.ably_sno}},{{.consumer_origin}},{{.price_origin}},{{.consumer_price_adjustment_discount_price}},{{.consumer_price_adjustment_discount_rate}},{{.consumer_price_adjustment_discount_type}},"{{.consumer_price_adjustment_started_at}}","{{.consumer_price_adjustment_ended_at}}","{{.asctime}}",{{.message}}`' \
  >o.jsonl

