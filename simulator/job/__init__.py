from .Job import Request, Job, JobWork, JobReconfiguration, JobSystem, JobSystemTurnOn, JobSystemTurnOff, JobSystemFrequency


def generate_factor_linear(a: float, b: float):
    return lambda x: a * x + b


def generate_factor_quadratic(a: float, b: float, c: float):
    return lambda x: a * x * x + b * x + c
