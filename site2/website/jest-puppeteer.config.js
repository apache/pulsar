module.exports = {
  launch: {
    headless: true,
  },
  server: {
    command: 'yarn start --no-watch',
    port: 3000,
    launchTimeout: 10000,
  },
}
