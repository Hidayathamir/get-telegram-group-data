from typing import Any, Dict
from telethon import TelegramClient
from telethon.tl.patched import Message
import pandas as pd
import yaml


async def extract_data_from_message(message: Message) -> Dict[str, Any]:
    data = {}

    data["id"] = message.id
    data["reply_to_msg_id"] = message.reply_to_msg_id
    data["text"] = message.text

    sender = message.sender
    if not sender is None:
        data["first_name"] = sender.first_name
        data["last_name"] = sender.last_name
        data["username"] = sender.username
        data["phone"] = sender.phone

    date = message.date
    data["day_utc"] = date.day
    data["month_utc"] = date.month
    data["year_utc"] = date.year
    data["hour_utc"] = date.hour
    data["minute_utc"] = date.minute
    data["second_utc"] = date.second

    return data


def preprocess(df: pd.DataFrame) -> pd.DataFrame:
    str_columns = [
        "text",
        "first_name",
        "last_name",
    ]
    int_columns = [
        "id",
        "reply_to_msg_id",
        "phone",
        "day_utc",
        "month_utc",
        "year_utc",
        "hour_utc",
        "minute_utc",
        "second_utc",
    ]
    df[str_columns] = df[str_columns].fillna("")
    df.text = df.text.apply(lambda x: x.replace("\n", "\\n"))
    df[int_columns] = df[int_columns].fillna(0.0).astype(int)
    return df


async def iter_messages(
    client: TelegramClient,
    target_username: str,
    min_id: int = 0,
    max_id: int = 0,
) -> pd.DataFrame:
    min_id -= 1
    max_id += 1
    df = pd.DataFrame()

    async for message in client.iter_messages(
        target_username, reverse=True, min_id=min_id, max_id=max_id
    ):
        df = df.append(
            await extract_data_from_message(message), ignore_index=True
        )

    df = preprocess(df)
    return df


def main(
    session_name: str,
    api_id: int,
    api_hash: str,
    target_username: str,
    min_id: int,
    max_id: int,
) -> None:
    client = TelegramClient(session_name, api_id, api_hash)
    with client:
        df = client.loop.run_until_complete(
            iter_messages(client, target_username, min_id, max_id)
        )

    csv_name = f"{target_username}-{min_id}-{max_id}.csv"
    df.to_csv(csv_name, index=False)


if __name__ == "__main__":
    with open("my_yaml.yaml", "r") as yaml_file:
        environ = yaml.safe_load(yaml_file)

    main(
        session_name=environ["session"],
        api_id=environ["api_id"],
        api_hash=environ["api_hash"],
        target_username="pythonID",
        min_id=2123,
        max_id=2142,
    )
