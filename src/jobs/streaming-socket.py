import json
import socket
import time
import pandas as pd


def send_data_over_socket(file_path, host="spark-master", port=9999, chunk_size=2):
    # Create a socket object
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    print("Socket successfully created")
    print(host, port)
    s.bind((host, port))
    print("Socket binded to %s" % (port))

    s.listen(1)
    print(f"Socket is listening on {host}:{port}")

    

    last_sent_index = 0

    while True:
        # Establish a connection with client
        conn, addr = s.accept()
        print(f"Got a connection from {addr}")

        try:
            with open(file_path, "rb") as file:
                # skip the lines that were already processed
                for _ in range(last_sent_index):
                    next(file)

                records = []
                for line in file:
                    records.append(json.loads(line))
                    if len(records) == chunk_size:
                        chunk = pd.DataFrame(records)
                        print(chunk)
                        for record in chunk.to_dict(orient="records"):
                            serialized_data = json.dumps(record).encode("utf-8")
                            conn.send(serialized_data + b"\n")
                            time.sleep(5)
                            last_sent_index += 1

                            records = []

        except (BrokenPipeError, ConnectionResetError):
            print("Client disconnected")
        finally:
            conn.close()
            print("Socket closed")


if __name__ == "__main__":
    send_data_over_socket("datasets/yelp_academic_dataset_review.json")