To configure:
```bash
vi ./data_api/config.py
```

To run tests:
```bash
bash ./script/test.sh
```


To insert data:
```bash
python -m data_api_core.bin.insert_glob '/data/wghilliard/*.h5' --label=all --backend=file --family=baseline --data_sets=thing1,thing2,thing3
```

Notes:

- Testing PyCharm may not work due to pathing issues.
Not sure how to resolve, testing with `pytest` in the root
directory will ensure tests pass.