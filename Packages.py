import importlib.util

required_modules = [
    "requests",
    "mysql.connector",
    "numpy",
    "pandas",
    "yaml",
    "flask",
    "dotenv",
    "statsmodels",
    "sklearn.ensemble"
]

for module in required_modules:
    if importlib.util.find_spec(module) is None:
        print(f"Module {module} is not installed.")
    else:
        print(f"Module {module} is installed.")