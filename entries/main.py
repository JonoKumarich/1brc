import mmap
import multiprocessing as mp
import os

FILE_PATH = "./measurements.txt"
CPU_COUNT = cpus if (cpus := os.cpu_count()) else 1
# CPU_COUNT = 1
FILE_SIZE = os.stat(FILE_PATH).st_size
CHUNK_SIZE = FILE_SIZE // CPU_COUNT


def calculate_metrics(path: str, start: int, end: int):
    metrics: dict[bytes, tuple[int, float, float, float]] = {}

    with open(path, "rb") as f:
        f.seek(start)
        while line := f.readline().rstrip(b"\n"):
            city, value = line.split(b";")
            value = float(value)

            if city not in metrics:
                metrics[city] = (1, value, value, value)
                continue

            existing = metrics[city]
            metrics[city] = (
                existing[0] + 1,
                existing[1] + value,
                min(existing[2], value),
                max(existing[3], value),
            )

            if f.tell() >= end - 1:
                return metrics

    # ERROR: We are returning here for some reason
    assert False, f"We should have returned earlier {start}, {end}"


def print_metrics(metrics: dict[bytes, tuple[int, float, float, float]]):
    output = dict(sorted(metrics.items()))

    for city, (n, sum, min_val, max_val) in output.items():
        print(f"{city.decode()}={min_val:.1f}/{sum/n:.1f}/{max_val:.1f}")


def get_chunk_args(file: mmap.mmap) -> list[tuple[str, int, int]]:
    bounds = []
    chunk_end = 0
    for _ in range(CPU_COUNT):
        chunk_start = chunk_end
        chunk_end = min(chunk_end + CHUNK_SIZE, FILE_SIZE)
        chunk_end = (
            file.find(b"\n", chunk_end) + 1 if chunk_end < FILE_SIZE else FILE_SIZE
        )

        bounds.append((FILE_PATH, chunk_start, chunk_end))

    return bounds


def merge_results(
    results: list[dict[bytes, tuple[int, float, float, float]]],
) -> dict[bytes, tuple[int, float, float, float]]:
    merged = {}
    for result in results:
        for city, (n, sum, min_val, max_val) in result.items():
            if city not in merged:
                merged[city] = (n, sum, min_val, max_val)
                continue

            existing = merged[city]
            merged[city] = (
                existing[0] + n,
                existing[1] + sum,
                min(existing[2], min_val),
                max(existing[3], max_val),
            )

    return merged


if __name__ == "__main__":
    import time

    start = time.time()
    with mmap.mmap(os.open(FILE_PATH, os.O_RDONLY), 0, prot=mmap.PROT_READ) as file:
        bounds = get_chunk_args(file)

    procs: list[mp.Process] = []

    with mp.Pool(processes=CPU_COUNT) as pool:
        res = pool.starmap(calculate_metrics, bounds)

    end = time.time()
    metrics = merge_results(res)
    print_metrics(metrics)
    print(end - start)
