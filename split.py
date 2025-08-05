import json
import random
import re
import string
import subprocess
import sys
from typing import Any

CREATED_TOPICS = []
PASSED_MESSAGE_TO_TMP = False
PASSED_MESSAGE_TO_TMP_FALLBACK = False


def tmp_topic_name(original: str, is_fallback: bool) -> str:
    """Generates a name for a temporary mirrord topic."""

    random_digits = "".join(random.choices(string.digits, k=10))
    fallback = "-"
    if is_fallback:
        fallback = "-fallback-"

    return f"mirrord-tmp-{random_digits}{fallback}{original}"


def create_topic(original: str, is_fallback: bool, partitions: int) -> str:
    """Creates a temporary mirrord topic."""

    name = tmp_topic_name(original, is_fallback)
    print(
        f"Creating temporary topic `{name}`...",
        end="",
    )
    subprocess.run(
        [
            "kafkactl",
            "create",
            "topic",
            name,
            "--partitions",
            str(partitions),
            "--replication-factor",
            "1",
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    print(f" ✅")

    CREATED_TOPICS.append(name)

    return name


def try_json(buffer: str) -> Any:
    """Tries to parse the given string as JSON. Returns None in case a parsing error occurs."""

    try:
        return json.loads(buffer)
    except json.JSONDecodeError:
        return None


def run_split(args):
    global PASSED_MESSAGE_TO_TMP
    global PASSED_MESSAGE_TO_TMP_FALLBACK

    topic_name = args.topic
    header_name = args.header_name
    header_value_regex = args.header_value_regex
    compiled_regex = re.compile(args.header_value_regex)

    print(
        f"--- Simulating a split of topic `{topic_name}` using filter `{header_name}: {header_value_regex}` ---\n"
    )

    print(f"Fetching metadata of topic `{topic_name}`...", end="")
    result = subprocess.run(
        ["kafkactl", "describe", "topic", topic_name, "--output", "json"],
        capture_output=True,
        text=True,
        check=True,
    )
    description = json.loads(result.stdout)
    partitions = len(description.get("Partitions", []))
    print(f" {partitions} partition(s) ✅")

    tmp_fallback_topic = create_topic(topic_name, True, partitions)
    tmp_topic = create_topic(topic_name, False, partitions)
    print("")

    print(f"Reading messages from topic `{topic_name}`...")
    print(
        f"Messages with header `{header_name}` matching regex `{header_value_regex}` will be sent to `{tmp_topic}`"
    )
    print(f"All other messages will be sent to `{tmp_fallback_topic}`")
    print(f"Please produce message to topic `{topic_name}`.")
    print(
        f"When you verify that the script is able to pass messages to both temporary topics, press Ctrl+C to stop forwarding.\n"
    )

    consume_cmd = [
        "kafkactl",
        "consume",
        topic_name,
        "-g",
        args.consumer_group,
        "-o",
        "json",
        "--print-headers",
        "-k",
        "--key-encoding",
        "hex",
        "--value-encoding",
        "hex",
    ]
    consumer = subprocess.Popen(
        consume_cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1024,
    )

    buffer = ""
    for line in consumer.stdout:
        if not line:
            continue
        buffer += line
        as_json = try_json(buffer)
        if as_json is None:
            continue
        buffer = ""

        print(f"Received a message: {as_json}")
        partition = as_json["Partition"]
        headers = as_json.get("Headers", {})
        key = as_json.get("Key", None)
        value = as_json.get("Value", None)

        send_to = None
        for h_name, h_value in headers.items():
            if h_name == header_name and compiled_regex.match(h_value):
                print(
                    f"Message matches the filter, sending it to topic `{tmp_topic}`...",
                    end="",
                )
                send_to = tmp_topic
                PASSED_MESSAGE_TO_TMP = True
                break
        else:
            print(
                f"Message does not match the filter, sending it to topic `{tmp_fallback_topic}`...",
                end="",
            )
            send_to = tmp_fallback_topic
            PASSED_MESSAGE_TO_TMP_FALLBACK = True

        produce_cmd = ["kafkactl", "produce", send_to, "--partition", str(partition)]
        if key is not None:
            produce_cmd += ["-k", key, "--key-encoding", "hex"]
        if value is not None:
            produce_cmd += ["-v", value, "--value-encoding", "hex"]
        for h_name, h_value in headers.items():
            produce_cmd += ["-H", f"{h_name}:{h_value}"]

        subprocess.run(
            produce_cmd,
            capture_output=True,
            text=True,
            check=True,
        )
        print(f" ✅")

    stderr = consumer.stderr.read()
    result = consumer.wait()
    if result != 0:
        raise subprocess.CalledProcessError(
            result,
            " ".join(consume_cmd),
            output=None,
            stderr=stderr,
        )


def run_cleanup(args):
    print(f"Listing temporary mirrord topics...", end="")
    result = subprocess.run(
        ["kafkactl", "get", "topics", "--output", "json"],
        capture_output=True,
        text=True,
        check=True,
    )
    print(f" ✅")

    topics = json.loads(result.stdout)
    names = []
    for topic in topics:
        name = topic.get("Name", None)
        if name is not None and name.startswith("mirrord-tmp"):
            names.append(name)
    print(f"Found temporary mirrord topics: {names}")

    for name in names:
        print(f"Deleting topic `{name}`...", end="")
        subprocess.run(
            ["kafkactl", "delete", "topic", name],
            capture_output=True,
            text=True,
            check=True,
        )
        print(f" ✅")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Using kafkactl, check whether Kafka credentials are sufficient for the mirrord Operator to run Kafka splitting.",
        epilog="Before using this script, please make sure that kafkactl is properly configured and available in path.",
    )
    subparser = parser.add_subparsers(dest="subcommand", required=True)

    split_p = subparser.add_parser(
        "split",
        help="verify current kafkactl credentials by simulating a Kafka topic split",
    )
    split_p.add_argument(
        "--topic", help="name of an existing Kafka topic to split", required=True
    )
    split_p.add_argument(
        "--consumer-group",
        help="consumer group to use when reading messages from the existing Kafka topic",
        required=True,
    )
    split_p.add_argument(
        "--header-name",
        help="name of the header to use when routing messages based on header value",
        required=True,
    )
    split_p.add_argument(
        "--header-value-regex",
        help="regex to use when routing messages based on header value",
        required=True,
    )
    split_p.set_defaults(func=run_split)

    cleanup_p = subparser.add_parser(
        "cleanup", help="cleanup all temporary mirrord Kafka topics"
    )
    cleanup_p.set_defaults(func=run_cleanup)

    args = parser.parse_args()

    try:
        args.func(args)
    except FileNotFoundError as e:
        print(e, file=sys.stderr)
        print(
            "❌ kafkactl was not found. Please make sure that kafkactl is installed and available in path.",
            file=sys.stderr,
        )
        sys.exit(1)

    except subprocess.CalledProcessError as e:
        print("❌ kafkactl command failed:", file=sys.stderr)
        print(str(e), file=sys.stderr)
        print(f"Stderr: {e.stderr}", file=sys.stderr)
        print("Please make sure that kafkactl is properly configured.", file=sys.stderr)
        sys.exit(1)

    except re.error as e:
        print(f"❌ Invalid header value regex: {e}")
        sys.exit(1)

    except KeyboardInterrupt:
        print(f"Captured keyboard interrup, exiting...")
        for topic in CREATED_TOPICS:
            print(f"Deleting topic `{topic}`...", end="")
            subprocess.run(
                ["kafkactl", "delete", "topic", topic],
                capture_output=True,
                text=True,
                check=True,
            )
            print(f" ✅")

        exit_code = 0
        if not PASSED_MESSAGE_TO_TMP:
            print(
                f"No message passed the header filter, sending messages to tmp topics was not fully checked ❗"
            )
            exit_code = 1
        if not PASSED_MESSAGE_TO_TMP_FALLBACK:
            print(
                f"No message failed the header filter, sending messages to tmp topics was not fully checked ❗"
            )
            exit_code = 1
        if PASSED_MESSAGE_TO_TMP and PASSED_MESSAGE_TO_TMP_FALLBACK:
            print(
                f"\nKafka credentials are fully functional, topic was split successfully ✅"
            )

        sys.exit(exit_code)
