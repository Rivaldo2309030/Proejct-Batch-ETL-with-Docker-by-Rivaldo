def normalize_field_name(name):
    """
    Convierte un campo a snake_case, elimina espacios y caracteres especiales.
    """
    import re
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    snake = re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()
    snake = snake.replace(" ", "_")
    return snake

def safe_division(numerator, denominator):
    """
    División segura que devuelve None si el denominador es cero.
    """
    try:
        return numerator / denominator if denominator != 0 else None
    except Exception:
        return None
