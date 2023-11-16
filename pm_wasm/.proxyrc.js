
module.exports = function (app) {
  app.use(function middleware(req, res, next){ 
    
    // Set required headers to enable SharedArrayBuffer transfer in Worker.postMessage

    res.setHeader("Cross-Origin-Resource-Policy","same-origin");
    res.setHeader("Cross-Origin-Embedder-Policy","require-corp");
    res.setHeader("Cross-Origin-Opener-Policy","same-origin");

    next()
  });
};