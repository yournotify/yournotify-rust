# Yournotify Rust SDK

```rust
use serde_json::json;
use yournotify::Client;

let client = Client::new(std::env::var("YOURNOTIFY_API_KEY")?)?;
client.identify(json!({"external_id": "contact_123", "email": "person@example.com"})).await?;
client.track(json!({"event": "order.completed", "external_id": "contact_123", "idempotency_key": "order_123"})).await?;
client.voice().send(json!({"name": "Order update", "from": "Feedcover", "lists": ["+2348012345678"], "message": "Your order is ready"})).await?;
```

All namespaces are lowercase and the SDK supports safe retries, idempotency headers, structured errors and signed webhook verification.
