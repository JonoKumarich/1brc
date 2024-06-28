import mmap
import multiprocessing as mp
import os
import time

FILE_PATH = "./measurements.txt"
CPU_COUNT = cpus if (cpus := os.cpu_count()) else 1
FILE_SIZE = os.stat(FILE_PATH).st_size
CHUNK_SIZE = FILE_SIZE // CPU_COUNT


def calculate_metrics(path: str, start: int, end: int):
    metrics: dict[bytes, tuple[int, int, float, float]] = {}
    mmap_chunk_start = (
        start // mmap.ALLOCATIONGRANULARITY
    ) * mmap.ALLOCATIONGRANULARITY

    with mmap.mmap(
        fileno=os.open(path, os.O_RDONLY),
        length=end - mmap_chunk_start,
        offset=mmap_chunk_start,
        access=mmap.ACCESS_READ,
    ) as mm:
        mm.seek(start - mmap_chunk_start)

        while line := mm.readline():
            sep = line.find(b";")

            city = line[:sep]
            value = int(line[sep + 1 : -3] + line[-2:-1])

            if city in metrics:
                n, total, lower, upper = metrics[city]
                metrics[city] = (
                    n + 1,
                    total + value,
                    min(lower, value),
                    max(upper, value),
                )
            else:
                metrics[city] = (1, value, value, value)

    return metrics


def print_metrics(metrics: dict[bytes, tuple[int, int, float, float]]):
    output = dict(sorted(metrics.items()))

    for city, (n, sum, min_val, max_val) in output.items():
        print(f"{city.decode()}={min_val/10:.1f}/{sum/(n*10):.1f}/{max_val/10:.1f}")


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
    results: list[dict[bytes, tuple[int, int, float, float]]],
) -> dict[bytes, tuple[int, int, float, float]]:
    merged = {}
    for result in results:
        for city, (n, sum, min_val, max_val) in result.items():
            if city not in merged:
                merged[city] = (n, sum, min_val, max_val)
            else:
                existing = merged[city]
                merged[city] = (
                    existing[0] + n,
                    existing[1] + sum,
                    min(existing[2], min_val),
                    max(existing[3], max_val),
                )

    return merged


if __name__ == "__main__":
    start = time.time()
    with mmap.mmap(os.open(FILE_PATH, os.O_RDONLY), 0, prot=mmap.PROT_READ) as file:
        bounds = get_chunk_args(file)

    with mp.Pool(processes=CPU_COUNT) as pool:
        res = pool.starmap(calculate_metrics, bounds)

    end = time.time()
    metrics = merge_results(res)
    print_metrics(metrics)
    print(end - start)
