import logging

# Create a logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Create handlers
console_handler = logging.StreamHandler()

# Create a formatter
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

# Set formatter for handlers
console_handler.setFormatter(formatter)

# Add handlers to the logger
logger.addHandler(console_handler)