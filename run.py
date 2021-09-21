from multiprocessing import Process
from typing import List, Tuple
from main import main
import yaml


def split_range_id_according_to_number_of_core(
    alist: range, num_of_core: int = 4
) -> Tuple[List[int], List[int]]:
    length = len(alist)
    min_ids = []
    max_ids = []

    for i in range(num_of_core):
        min = i * length // num_of_core
        max = (i + 1) * length // num_of_core
        j = alist[min:max]
        min_ids.append(j[0])
        max_ids.append(j[-1])

    return min_ids, max_ids


def run_main_in_multiprocess(
    group_link: str,
    min_id: int,
    max_id: int,
    api_id: int,
    api_hash: str,
    session_name: List[str],
) -> None:
    min_ids, max_ids = split_range_id_according_to_number_of_core(
        range(min_id, max_id + 1), num_of_core=4
    )
    processes = []

    for session, min_id, max_id in zip(session_name, min_ids, max_ids):
        processes.append(
            Process(
                target=main,
                args=(
                    session,
                    api_id,
                    api_hash,
                    group_link,
                    min_id,
                    max_id,
                ),
            )
        )

    for p in processes:
        p.start()

    for p in processes:
        p.join()


if __name__ == "__main__":
    # Example how to run main in multiprocess
    with open("my_yaml.yaml", "r") as yaml_file:
        environ = yaml.safe_load(yaml_file)

    # create your seassion first and use it in here
    # i have 4 core, so i create 4 seassion with defferent name
    # in my case, i have
    # core1.session
    # core2.session
    # core3.session
    # core4.session
    run_main_in_multiprocess(
        session_name=["core1", "core2", "core3", "core4"],
        api_id=environ["api_id"],
        api_hash=environ["api_hash"],
        group_link="https://t.me/pythonID",
        min_id=0,
        max_id=240000,
    )
