import { resolve } from "path";

const { createVuePlugin } = require("vite-plugin-vue2");

module.exports = {
  plugins: [createVuePlugin(/*options*/)],
  resolve: {
    alias: {
      "@": resolve(__dirname, "src"), // 设置 `@` 指向 `src` 目录
      "@comp": resolve(__dirname, "src/components"),
    },
  },
  // base: './', // 设置打包路径
  // base: import.meta.env.NODE_ENV === "production" ? "/xxx" : "./", // 设置打包路径
  server: {
    host:'0.0.0.0',
    port: 4001, // 设置服务启动端口号
    historyApiFallback: true,
    allowedHosts: 'all',
    client: {
      overlay: true,
      reconnect: false,
      webSocketURL: 'ws://0.0.0.0:4001/ws',
    },
    open: true, // 设置服务启动时是否自动打开浏览器
    cors: true, // 允许跨域
    proxy: {
      "/aidb": {
        target: "http://127.0.0.1:5000",
        changeOrigin: true,
        proxyTimeout: 10 * 60 * 1000,
        timeout: 10 * 60 * 1000,
        // pathRewrite: {"^/aidb" : "aidb"}
        rewrite: (path) => path.replace(/^\/aidb/, "aidb"),
      },
    },
  },
};
