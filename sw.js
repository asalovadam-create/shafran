/*
  SHAFRAN service worker.
  Стратегия:
  - "оболочка" сайта (html/css/js/шрифты/картинки логотипа и фона) — cache-first,
    поэтому после первого визита сайт открывается мгновенно, даже на плохой сети;
  - все запросы к /api/* и /ws — всегда идут в сеть напрямую (никогда не кэшируются),
    иначе таймер и номера будут "залипать";
  - если появляется новая версия оболочки — она тихо подгружается в фоне
    и подменяет кэш для следующего визита (stale-while-revalidate).
*/

const CACHE_NAME = "shafran-shell-v1";
const SHELL_FILES = [
  "/",
  "/static/index.html",
  "/static/bg.jpg",
  "/static/logo.png",
  "/manifest.json",
];

self.addEventListener("install", (event) => {
  event.waitUntil(
    caches.open(CACHE_NAME).then((cache) =>
      cache.addAll(SHELL_FILES).catch(() => {})
    )
  );
  self.skipWaiting();
});

self.addEventListener("activate", (event) => {
  event.waitUntil(
    caches.keys().then((keys) =>
      Promise.all(keys.filter((k) => k !== CACHE_NAME).map((k) => caches.delete(k)))
    )
  );
  self.clients.claim();
});

self.addEventListener("fetch", (event) => {
  const url = new URL(event.request.url);

  // API и WebSocket — никогда не кэшируем, всегда свежие данные
  if (url.pathname.startsWith("/api/") || url.pathname.startsWith("/ws")) {
    return;
  }

  if (event.request.method !== "GET") return;

  event.respondWith(
    caches.match(event.request).then((cached) => {
      const network = fetch(event.request)
        .then((response) => {
          if (response && response.ok) {
            const clone = response.clone();
            caches.open(CACHE_NAME).then((cache) => cache.put(event.request, clone));
          }
          return response;
        })
        .catch(() => cached);
      return cached || network;
    })
  );
});
