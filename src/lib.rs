use std::time::{Duration, SystemTime, UNIX_EPOCH};
use hmac::{Hmac, Mac};
use reqwest::{Method, StatusCode};
use serde_json::{json, Value};
use sha2::Sha256;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Yournotify API key is required")]
    MissingApiKey,
    #[error("Yournotify API request failed with status {status}: {message}")]
    Api { status: u16, message: String, body: Value, request_id: Option<String> },
    #[error(transparent)]
    Http(#[from] reqwest::Error),
}

#[derive(Clone)]
pub struct Client {
    api_key: String,
    base_url: String,
    http: reqwest::Client,
    max_retries: usize,
}

impl Client {
    pub fn new(api_key: impl Into<String>) -> Result<Self, Error> {
        let api_key = api_key.into();
        if api_key.trim().is_empty() { return Err(Error::MissingApiKey); }
        Ok(Self { api_key, base_url: "https://api.yournotify.com/".into(), http: reqwest::Client::builder().timeout(Duration::from_secs(30)).build()?, max_retries: 2 })
    }
    pub fn with_base_url(mut self, value: impl Into<String>) -> Self { self.base_url = format!("{}/", value.into().trim_end_matches('/')); self }
    pub fn with_max_retries(mut self, value: usize) -> Self { self.max_retries = value; self }
    pub async fn request(&self, method: Method, endpoint: &str, data: Value) -> Result<Value, Error> {
        let url = format!("{}{}", self.base_url, endpoint.trim_start_matches('/'));
        let idempotency = data.get("idempotency_key").or_else(|| data.get("event_id")).and_then(Value::as_str).map(str::to_owned);
        let retryable = matches!(&method, &Method::GET | &Method::HEAD | &Method::PUT | &Method::DELETE) || idempotency.is_some();
        for attempt in 0..=self.max_retries {
            let mut req = self.http.request(method.clone(), &url).bearer_auth(&self.api_key).header("Accept", "application/json");
            if let Some(key) = &idempotency { req = req.header("Idempotency-Key", key); }
            req = if method == Method::GET { req.query(&data) } else { req.json(&data) };
            match req.send().await {
                Ok(response) => {
                    let status = response.status();
                    let request_id = response.headers().get("x-request-id").and_then(|v| v.to_str().ok()).map(str::to_owned);
                    let retry_after = response.headers().get("retry-after").and_then(|v| v.to_str().ok()).and_then(|v| v.parse::<u64>().ok());
                    let body = response.json::<Value>().await.unwrap_or(Value::Null);
                    if status.is_success() { return Ok(body); }
                    if !retryable || attempt == self.max_retries || (status != StatusCode::TOO_MANY_REQUESTS && !status.is_server_error()) {
                        return Err(Error::Api { status: status.as_u16(), message: body.get("message").and_then(Value::as_str).unwrap_or("Request failed").into(), body, request_id });
                    }
                    tokio::time::sleep(Duration::from_millis(retry_after.map(|v| v * 1000).unwrap_or(250 * (1 << attempt)))).await;
                }
                Err(_error) if retryable && attempt < self.max_retries => tokio::time::sleep(Duration::from_millis(250 * (1 << attempt))).await,
                Err(error) => return Err(Error::Http(error)),
            }
        }
        unreachable!()
    }
    pub async fn validate_auth(&self) -> Result<Value, Error> { self.request(Method::GET, "auth/me", json!({})).await }
    pub async fn identify(&self, data: Value) -> Result<Value, Error> { self.request(Method::POST, "sdk/identify", data).await }
    pub async fn track(&self, data: Value) -> Result<Value, Error> { self.request(Method::POST, "sdk/events", normalize_event(data)).await }
    pub async fn track_batch(&self, events: Vec<Value>, options: Value) -> Result<Value, Error> {
        let mut payload = options.as_object().cloned().unwrap_or_default(); payload.insert("events".into(), Value::Array(events.into_iter().map(normalize_event).collect()));
        self.request(Method::POST, "sdk/events/batch", Value::Object(payload)).await
    }
    pub async fn alias(&self, data: Value) -> Result<Value, Error> { self.request(Method::POST, "sdk/alias", data).await }
    pub fn email(&self) -> Channel<'_> { Channel { client: self, channel: "email" } }
    pub fn sms(&self) -> Channel<'_> { Channel { client: self, channel: "sms" } }
    pub fn whatsapp(&self) -> Channel<'_> { Channel { client: self, channel: "whatsapp" } }
    pub fn voice(&self) -> Channel<'_> { Channel { client: self, channel: "voice" } }
    pub fn push(&self) -> Channel<'_> { Channel { client: self, channel: "push" } }
    pub fn inapp(&self) -> Channel<'_> { Channel { client: self, channel: "inapp" } }
    pub fn contact(&self) -> Contact<'_> { Contact(self) }
    pub fn lists(&self) -> Lists<'_> { Lists(self) }
    pub fn rewards(&self) -> Rewards<'_> { Rewards(self) }
    pub fn loyalty(&self) -> Loyalty<'_> { Loyalty(self) }
    pub fn referrals(&self) -> Referrals<'_> { Referrals(self) }
}

fn normalize_event(mut data: Value) -> Value { if !data.is_object() { data=json!({}); } let object=data.as_object_mut().unwrap(); if !object.contains_key("event_id") { let id=object.get("idempotency_key").cloned().unwrap_or_else(|| Value::String(uuid::Uuid::new_v4().to_string())); object.insert("event_id".into(),id); } if !object.contains_key("occurred_at") { object.insert("occurred_at".into(),Value::String(chrono::Utc::now().to_rfc3339())); } data }

pub struct Channel<'a> { client: &'a Client, channel: &'static str }
impl Channel<'_> {
    pub async fn send(&self, mut data: Value) -> Result<Value, Error> {
        if self.channel == "voice" { return self.client.request(Method::POST, "campaigns/voice", data).await; }
        data.as_object_mut().map(|o| o.insert("channel".into(), self.channel.into()));
        self.client.request(Method::POST, "campaigns", data).await
    }
}

pub struct Contact<'a>(&'a Client);
impl Contact<'_> {
    pub async fn create(&self, data: Value) -> Result<Value, Error> { self.0.request(Method::POST, "contacts", data).await }
    pub async fn all(&self, params: Value) -> Result<Value, Error> { self.0.request(Method::GET, "contacts", params).await }
    pub async fn get(&self, id: impl std::fmt::Display) -> Result<Value, Error> { self.0.request(Method::GET, &format!("contacts/{id}"), json!({})).await }
    pub async fn update(&self, id: impl std::fmt::Display, data: Value) -> Result<Value, Error> { self.0.request(Method::PUT, &format!("contacts/{id}"), data).await }
    pub async fn delete(&self, id: impl std::fmt::Display) -> Result<Value, Error> { self.0.request(Method::DELETE, &format!("contacts/{id}"), json!({})).await }
    pub async fn summary(&self, params: Value) -> Result<Value, Error> { self.0.request(Method::GET, "contacts/summary", params).await }
    pub async fn create_session(&self, data: Value) -> Result<Value, Error> { self.0.request(Method::POST, "contacts/session", data).await }
}

pub struct Lists<'a>(&'a Client);
impl Lists<'_> {
    pub async fn create(&self, data: Value) -> Result<Value, Error> { self.0.request(Method::POST, "lists", data).await }
    pub async fn all(&self, params: Value) -> Result<Value, Error> { self.0.request(Method::GET, "lists", params).await }
    pub async fn get(&self, id: impl std::fmt::Display) -> Result<Value, Error> { self.0.request(Method::GET, &format!("lists/{id}"), json!({})).await }
    pub async fn update(&self, id: impl std::fmt::Display, data: Value) -> Result<Value, Error> { self.0.request(Method::PUT, &format!("lists/{id}"), data).await }
    pub async fn delete(&self, id: impl std::fmt::Display) -> Result<Value, Error> { self.0.request(Method::DELETE, &format!("lists/{id}"), json!({})).await }
    pub async fn export(&self, id: impl std::fmt::Display) -> Result<Value, Error> { self.0.request(Method::GET, &format!("lists/export/{id}"), json!({})).await }
}

pub struct Rewards<'a>(&'a Client);
impl Rewards<'_> { pub async fn all(&self,p:Value)->Result<Value,Error>{self.0.request(Method::GET,"rewards",p).await} pub async fn get(&self,id:impl std::fmt::Display)->Result<Value,Error>{self.0.request(Method::GET,&format!("rewards/{id}"),json!({})).await} pub async fn create(&self,d:Value)->Result<Value,Error>{self.0.request(Method::POST,"rewards",d).await} pub async fn update(&self,id:impl std::fmt::Display,d:Value)->Result<Value,Error>{self.0.request(Method::PUT,&format!("rewards/{id}"),d).await} pub async fn delete(&self,id:impl std::fmt::Display)->Result<Value,Error>{self.0.request(Method::DELETE,&format!("rewards/{id}"),json!({})).await} pub async fn issue(&self,d:Value)->Result<Value,Error>{self.0.request(Method::POST,"rewards/send",d).await} pub async fn products(&self,p:Value)->Result<Value,Error>{self.0.request(Method::GET,"rewards/products",p).await} }
pub struct Loyalty<'a>(&'a Client);
impl Loyalty<'_> { pub async fn programs(&self,p:Value)->Result<Value,Error>{self.0.request(Method::GET,"loyalty/programs",p).await} pub async fn create_program(&self,d:Value)->Result<Value,Error>{self.0.request(Method::POST,"loyalty/programs",d).await} pub async fn track(&self,id:impl std::fmt::Display,d:Value)->Result<Value,Error>{self.0.request(Method::POST,&format!("loyalty/programs/{id}/events"),d).await} pub async fn adjust(&self,id:impl std::fmt::Display,d:Value)->Result<Value,Error>{self.0.request(Method::POST,&format!("loyalty/programs/{id}/points"),d).await} pub async fn redeem(&self,id:impl std::fmt::Display,d:Value)->Result<Value,Error>{self.0.request(Method::POST,&format!("loyalty/programs/{id}/redeem"),d).await} }
pub struct Referrals<'a>(&'a Client);
impl Referrals<'_> { pub async fn programs(&self,p:Value)->Result<Value,Error>{self.0.request(Method::GET,"referrals/programs",p).await} pub async fn create_program(&self,d:Value)->Result<Value,Error>{self.0.request(Method::POST,"referrals/programs",d).await} pub async fn track(&self,id:impl std::fmt::Display,d:Value)->Result<Value,Error>{self.0.request(Method::POST,&format!("referrals/programs/{id}/events"),d).await} pub async fn analytics(&self,id:impl std::fmt::Display,p:Value)->Result<Value,Error>{self.0.request(Method::GET,&format!("referrals/programs/{id}/analytics"),p).await} }

pub fn verify_webhook(payload: &[u8], signature: &str, timestamp: &str, secret: &str, tolerance: Duration) -> bool {
    let parts: std::collections::HashMap<_, _> = signature.split(',').filter_map(|part| part.split_once('=')).collect();
    let timestamp = if timestamp.is_empty() { parts.get("t").copied().unwrap_or("") } else { timestamp };
    let signature = parts.get("v1").copied().unwrap_or(signature);
    let seconds = match timestamp.parse::<u64>() { Ok(value) => value, Err(_) => return false };
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
    if now.abs_diff(seconds) > tolerance.as_secs() { return false; }
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("HMAC accepts any key size");
    mac.update(timestamp.as_bytes()); mac.update(b"."); mac.update(payload);
    let supplied = signature.strip_prefix("sha256=").unwrap_or(signature);
    hex::decode(supplied).map(|bytes| mac.verify_slice(&bytes).is_ok()).unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn exposes_all_resources() {
        let sdk = Client::new("test").unwrap();
        let _ = (sdk.email(), sdk.sms(), sdk.whatsapp(), sdk.voice(), sdk.push(), sdk.inapp(), sdk.contact(), sdk.lists(), sdk.rewards(), sdk.loyalty(), sdk.referrals());
    }
    #[test]
    fn verifies_signed_webhooks() {
        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs().to_string();
        let body = br#"{"event":"reward.fulfilled"}"#;
        let mut mac = Hmac::<Sha256>::new_from_slice(b"secret").unwrap();
        mac.update(timestamp.as_bytes()); mac.update(b"."); mac.update(body);
        let signature = format!("t={},v1={}", timestamp, hex::encode(mac.finalize().into_bytes()));
        assert!(verify_webhook(body, &signature, "", "secret", Duration::from_secs(300)));
        assert!(!verify_webhook(b"changed", &signature, "", "secret", Duration::from_secs(300)));
    }
}
