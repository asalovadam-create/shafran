/*
  SHAFRAN service worker.
  Стратегия:
  - HTML (сама страница) — network-first: сначала всегда пробуем сеть,
    чтобы человек мгновенно видел последнюю версию сайта после каждого
    обновления, и только если сети совсем нет — отдаём то, что есть в кэше.
  - статичные файлы (фон, логотип, иконки, manifest) — cache-first,
    они меняются редко, поэтому грузим их с диска устройства мгновенно.
  - /api/* и /ws — никогда не кэшируются, всегда идут в сеть напрямую.

  ВАЖНО: при каждом обновлении дизайна меняйте CACHE_NAME (например v2, v3…) —
  это гарантированно сбрасывает старый кэш у всех, кто уже открывал сайт.
*/

const CACHE_NAME = "shafran-shell-v2";
const STATIC_FILES = [
  "/static/bg.jpg",
  "/static/logo.png",
  "/manifest.json",
];

self.addEventListener("install", (event) => {
  event.waitUntil(
    caches.open(CACHE_NAME).then((cache) =>
      cache.addAll(STATIC_FILES).catch(() => {})
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

  if (url.pathname.startsWith("/api/") || url.pathname.startsWith("/ws")) {
    return; // всегда сеть, никогда не кэшируем
  }
  if (url.pathname === "/static/contact-photo.png") {
    return; // всегда свежая версия, без кэша (см. cache-busting в index.html)
  }
  if (event.request.method !== "GET") return;

  const isHTML =
    event.request.mode === "navigate" ||
    url.pathname === "/" ||
    url.pathname.endsWith(".html");

  if (isHTML) {
    // network-first: свежая версия сайта всегда в приоритете
    event.respondWith(
      fetch(event.request)
        .then((response) => {
          if (response && response.ok) {
            const clone = response.clone();
            caches.open(CACHE_NAME).then((cache) => cache.put(event.request, clone));
          }
          return response;
        })
        .catch(() => caches.match(event.request))
    );
    return;
  }

  // остальное (картинки, manifest) — cache-first для мгновенной загрузки
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
