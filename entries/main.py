import time
from pathlib import Path

FILE_PATH = Path(__file__).parent.parent / "measurements.txt"


def process_metrics():
    metrics: dict[bytes, tuple[int, float, float, float]] = {}

    with open(FILE_PATH, "rb") as f:
        while line := f.readline().strip():
            # if line[-6] == b";":
            #     # Case
            #     val = int(line[-1])

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

    return metrics


if __name__ == "__main__":
    start = time.time()
    output = dict(sorted(process_metrics().items()))

    for city, (n, sum, min_val, max_val) in output.items():
        print(f"{city.decode()}={min_val:.1f}/{sum/n:.1f}/{max_val:.1f}")

    end = time.time()
    print(f"Time taken: {end - start:.1f}s")
