from telethon import TelegramClient
import yaml


async def show_latest_id(target_username: str) -> None:
    async for message in client.iter_messages(target_username, limit=1):
        print(message.id)


if __name__ == "__main__":
    # Example how to run show_latest_id
    with open("my_yaml.yaml", "r") as yaml_file:
        environ = yaml.safe_load(yaml_file)

    client = TelegramClient("core4", environ["api_id"], environ["api_hash"])

    with client:
        client.loop.run_until_complete(
            show_latest_id(target_username="pythonID")
        )
