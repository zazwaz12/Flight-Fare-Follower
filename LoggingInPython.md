# Python Logging

```python
import logging
```

## logging methods
* debug
* info
* warning
* error
* crititcal

**basicConfig(level=logging.INFO)**-> makes it return all thing info level and above
Default is: warning, error and critical

in basicConfig, that we can only add once add extra parameter...
```python	
	format = "%(asctime)s - "%levelname"s - %(message)s"
```

## To change the message
```python
x = 2
logging.debug(f"The value of x is {x}")
```
## To log errors easily
```python
	logging.exception("The message describing")
	#OR
	logging.error("The message describing", exc_info = True)
```
**logging.exception** includes the error message by default.
**logging.error** is better if we dont want to include the message.

### Convention is to have a logger in each python module
```python
	logger = logging.getLogger(__name__)
```

### A handler to keep record of the logs
```python
	handler = logging.FileHandler('test.log')
	#name is also the module calling the log
	formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
	handler.setFormatter(formatter)
	logger.addHandler(handler)
	logger.info("Now any message or error type can be run")
```






