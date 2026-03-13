# Python Example

This example shows how to consume the generated wheel.

## 1. Build the wheel

```bash
./build.sh python
```

This produces a platform wheel in `build/`, for example:

```text
build/bifrore-0.1.0-cp39-cp39-manylinux2014_x86_64.whl
```

## 2. Install the wheel

```bash
pip install build/bifrore-*.whl
```

## 3. Run the example

```bash
python examples/python/consumer.py
```
