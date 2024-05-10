FROM public.ecr.aws/amazonlinux/amazonlinux:2023

RUN dnf install -y python3.11

# Make port 8125 available to the world outside this container
EXPOSE 8125/udp

# Copy and run the entrpoint.
COPY entrypoint.py /entrypoint.py
CMD ["python3", "-u", "entrypoint.py"]
