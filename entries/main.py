import sys


def process_metrics(path: str):
    metrics: dict[bytes, tuple[int, float, float, float]] = {}

    with open(path, "rb") as f:
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

    return metrics


if __name__ == "__main__":
    path = sys.argv[1]
    output = dict(sorted(process_metrics(path).items()))

    for city, (n, sum, min_val, max_val) in output.items():
        print(f"{city.decode()}={min_val:.1f}/{sum/n:.1f}/{max_val:.1f}")
