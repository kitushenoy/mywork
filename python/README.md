# Instruction to install any additional packages 
# Install virtual environment
pip install virtualenv virtualenvwrapper
echo "# Virtual Environment Wrapper"
echo "VIRTUALENVWRAPPER_PYTHON=/usr/local/bin/python2" >> ~/.bash_profile
echo "source /usr/local/bin/virtualenvwrapper.sh" >> ~/.bash_profile
source ~/.bash_profile
  
############ For Python 2 ############
# Create virtual environment
mkvirtualenv facecourse-py2 -p python2
workon facecourse-py2
  
# Now install python libraries within this virtual environment
pip install numpy scipy matplotlib scikit-image scikit-learn ipython pandas
  
# Quit virtual environment
deactivate
######################################
  
############ For Python 3 ############
# Create virtual environment
mkvirtualenv facecourse-py3 -p python3
workon facecourse-py3
  
# Now install python libraries within this virtual environment
pip install numpy scipy matplotlib scikit-image scikit-learn ipython pandas
  
# Quit virtual environment
deactivate
######################################
# 
