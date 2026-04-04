const http = require("http");
const fs = require("fs");
const path = require("path");

const PORT = 8877;
const ROOT = __dirname;

const MIME = {
  ".html": "text/html; charset=utf-8",
  ".js":   "application/javascript; charset=utf-8",
  ".css":  "text/css; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".png":  "image/png",
  ".jpg":  "image/jpeg",
  ".svg":  "image/svg+xml",
  ".ico":  "image/x-icon",
};

http.createServer((req, res) => {
  let p = new URL(req.url, "http://localhost").pathname;
  if (p === "/") p = "/index.html";
  const fp = path.join(ROOT, p);
  if (!fp.startsWith(ROOT)) { res.writeHead(403); res.end("Forbidden"); return; }
  fs.readFile(fp, (err, data) => {
    if (err) { res.writeHead(404); res.end("Not found"); return; }
    const ext = path.extname(fp).toLowerCase();
    res.writeHead(200, {
      "Content-Type": MIME[ext] || "application/octet-stream",
      "Access-Control-Allow-Origin": "*",
    });
    res.end(data);
  });
}).listen(PORT, () => console.log("QR Pay preview on http://localhost:" + PORT));
