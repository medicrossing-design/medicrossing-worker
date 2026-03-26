console.log("Worker started at " + new Date().toISOString());

let counter = 0;
const interval = setInterval(() => {
    counter++;
    console.log(`[${counter}] Heartbeat at ${new Date().toISOString()}`);
}, 5000);

process.on('SIGINT', () => {
    console.log("SIGINT received - shutting down");
    clearInterval(interval);
    process.exit(0);
});

process.on('SIGTERM', () => {
    console.log("SIGTERM received - shutting down");
    clearInterval(interval);
    process.exit(0);
});

process.on('uncaughtException', (err) => {
    console.error("ERROR:", err.message);
    clearInterval(interval);
    process.exit(1);
});
