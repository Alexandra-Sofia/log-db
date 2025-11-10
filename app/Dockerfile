# Use a small base image (e.g. Alpine Linux)
FROM alpine:latest

# Create a working directory
WORKDIR /app

# Add a placeholder file (optional)
RUN echo "LogDB application container placeholder" > /app/README.txt

# Define the default command (keeps the container alive)
CMD ["tail", "-f", "/dev/null"]
