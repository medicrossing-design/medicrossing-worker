console.log("Worker started");

const interval = setInterval(() => {
    console.log(`${new Date().toISOString()}: Worker is alive`);
}, 5000);

// Graceful shutdown
process.on('SIGINT', () => {
    console.log('Received SIGINT, shutting down gracefully');
    clearInterval(interval);
    process.exit(0);
});

process.on('SIGTERM', () => {
    console.log('Received SIGTERM, shutting down gracefully');
    clearInterval(interval);
    process.exit(0);
});

// Catch errors
process.on('uncaughtException', (err) => {
    console.error('Uncaught Exception:', err);
    clearInterval(interval);
    process.exit(1);
});

process.on('unhandledRejection', (reason, promise) => {
    console.error('Unhandled Rejection at:', promise, 'reason:', reason);
    clearInterval(interval);
    process.exit(1);
});
