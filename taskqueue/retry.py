def retry_delay(attempt: int, base: float = 0.1, maximum: float = 5.0) -> float:
    return min(base * 2 ** max(0, attempt - 1), maximum)

