# Define a class inherit from an exception type
class CustomError(Exception):
    def __init__(self):
        # Set some exception infomation
        pass
    def updateDB(self,arg1,arg2):
	print " updated error",arg2,arg2
try:
    # Raise an exception with argument
    raise CustomError()
except CustomError as e: 
    # Catch the custom exception
    #print 'Error: ', arg.msg
    e.updateDB("test","test2")	
