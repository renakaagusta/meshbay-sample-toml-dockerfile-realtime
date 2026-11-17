import { applicationVersion, config } from "./config";

export function renderPage(): string {
  return `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>meshbay-sample-realtime</title>
<style>
  :root { color-scheme: light dark; }
  body { font-family: ui-sans-serif, system-ui, sans-serif; margin: 0 auto; max-width: 46rem; padding: 1.5rem; line-height: 1.5; }
  h1 { font-size: 1.4rem; margin: 0 0 0.5rem; }
  h2 { font-size: 1rem; margin: 1.5rem 0 0.5rem; }
  code { background: rgba(127, 127, 127, 0.18); border-radius: 0.25rem; padding: 0.05rem 0.3rem; }
  .state { font-weight: 600; }
  ol { margin: 0; padding-left: 1.25rem; font-family: ui-monospace, SFMono-Regular, monospace; font-size: 0.85rem; }
  ol li { white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
  form { display: flex; gap: 0.5rem; margin: 0.75rem 0; }
  input { flex: 1; padding: 0.45rem 0.6rem; }
  button { padding: 0.45rem 0.9rem; }
  p.hint { color: rgba(127, 127, 127, 0.9); font-size: 0.85rem; }
</style>
</head>
<body>
<h1>meshbay-sample-realtime</h1>
<p>replica <code id="replica">${config.replicaID}</code> · version <code>${applicationVersion}</code> · redis <code id="redis">unknown</code> · clients <code id="clients">-</code></p>

<h2>SSE <code>/events</code> — <span class="state" id="events-state">connecting</span></h2>
<ol id="ticks"></ol>

<h2>WebSocket <code>/ws</code> — <span class="state" id="socket-state">connecting</span></h2>
<form id="chat-form">
  <input id="chat-input" autocomplete="off" placeholder="message, broadcast to every replica" />
  <button type="submit">send</button>
</form>
<ol id="messages"></ol>
<p class="hint">Open this page in two tabs. If the tabs are served by different replicas, a message typed in one appears in
both with the other replica in front of it — that is the Redis pub/sub fan-out. When the service is redeployed the
socket closes and the page reconnects by itself; the replica in the header changes.</p>

<script>
  var replicaNode = document.getElementById("replica");
  var redisNode = document.getElementById("redis");
  var clientsNode = document.getElementById("clients");
  var eventsState = document.getElementById("events-state");
  var socketState = document.getElementById("socket-state");
  var ticks = document.getElementById("ticks");
  var messages = document.getElementById("messages");

  function prepend(list, text, limit) {
    var item = document.createElement("li");
    item.textContent = text;
    list.insertBefore(item, list.firstChild);
    while (list.children.length > limit) {
      list.removeChild(list.lastChild);
    }
  }

  async function refreshHealth() {
    try {
      var response = await fetch("/health", { cache: "no-store" });
      var body = await response.json();
      replicaNode.textContent = body.replica;
      redisNode.textContent = body.redis ? "connected" : "local only";
      clientsNode.textContent = String(body.clients);
    } catch (error) {
      redisNode.textContent = "unreachable";
    }
  }
  refreshHealth();
  setInterval(refreshHealth, 5000);

  function connectEvents() {
    var source = new EventSource("/events");
    source.addEventListener("hello", function () {
      eventsState.textContent = "open";
    });
    source.addEventListener("tick", function (event) {
      eventsState.textContent = "open";
      var tick = JSON.parse(event.data);
      prepend(ticks, tick.at + " replica=" + tick.replica + " seq=" + tick.sequence + " clients=" + tick.clients, 15);
    });
    source.addEventListener("error", function () {
      eventsState.textContent = "reconnecting";
    });
  }
  connectEvents();

  var socket = null;
  function connectSocket() {
    var scheme = location.protocol === "https:" ? "wss://" : "ws://";
    socket = new WebSocket(scheme + location.host + "/ws");
    socketState.textContent = "connecting";
    socket.addEventListener("open", function () {
      socketState.textContent = "open";
    });
    socket.addEventListener("message", function (event) {
      var message = JSON.parse(event.data);
      if (message.type === "hello") {
        socketState.textContent = "open as " + message.client;
        prepend(messages, message.at + " hello from replica " + message.replica + " (redis " + message.redis + ")", 25);
      }
      if (message.type === "chat") {
        prepend(messages, message.at + " " + message.origin + ": " + message.text, 25);
      }
    });
    socket.addEventListener("close", function (event) {
      socketState.textContent = "closed (" + event.code + "), reconnecting in 2s";
      setTimeout(connectSocket, 2000);
    });
    socket.addEventListener("error", function () {
      socketState.textContent = "error";
    });
  }
  connectSocket();

  document.getElementById("chat-form").addEventListener("submit", function (event) {
    event.preventDefault();
    var input = document.getElementById("chat-input");
    var text = input.value.trim();
    if (text === "" || socket === null || socket.readyState !== WebSocket.OPEN) {
      return;
    }
    socket.send(JSON.stringify({ type: "chat", text: text }));
    input.value = "";
  });
</script>
</body>
</html>
`;
}
