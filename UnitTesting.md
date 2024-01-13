# Writing UnitTest
> To increase confidence with updating code

```python
#Naming of test file
test_<python file name>

import unittest
#where calc is another file with all the defined methods
import <python file name>

unittest.Testcase

class TestCalc(unittest.TestCase):
	def test_add(self):
		self.assertEqual(add(5,10), 15)
		self.assertEqual(add(10,10), 20)
	
	def test_add(self):
		self.assertEqual(multiply(3, 5), 15)
		self.assertEqual(multiple(3, 0), 0)
```

## Has to be run through using
> python test_calc.py

### Add the clause
```python
		if __name__ == '__main__':
			unittest.main()
```

### When defining tests 
```python
	def test_add #... will work 
	def add_test #... will not work
```	
	where add() exist in the other file

### Testing that errors persist where they should
```python
	#with a not being 0 error
	self.assertRaises(ValueError, calc.divide, 10, 0)
	#OR
	with self.assertRaises(ValueError):
		calc.divide(10,0)
```

## DRY principle for unittesting

### setUp and tearDown run before each test
```python
	#This creates the object before every test is to be run
	def setUp(self):
		self.emp_1 = Employee("John", "Smith", 599)

	#This could be for removing files that may have been added from setUp
	def tearDown(self):
		pass

	#to add an equivalent for the whole file use:
	@classmethod
	def setUpClass(cls):
		print('setupClass')
	
	@classmethod
	def tearDownClass(cls):
		print('teardownClass')
```
	The decorater makes it "run the class not an instance of the class"...?

# BELOW WOULD BE A NICE TO HAVE
> Mocking - if a website is down the logging should persist

in the code file check if the response is ok
otherwise return 'Bad Response'

to do this in the test file
```python	
	from unittest.mock import patch
as another test
	def test_monthly_schedule(self):
		with patch('employee.requests.get') as mocked_get:
			mocked_get.return_value.ok = True
			mocked_get.return_value.text = 'Success'
```
... https://www.youtube.com/watch?v=6tNS--WetLI 34:05

			
