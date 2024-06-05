import subprocess

scripts = ["kafka-producer.py", "flink.py", "kafka-receiver.py"]
print(1)
for file in scripts:
    print(file)
    subprocess.run(["python3", file])
