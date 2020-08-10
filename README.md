# This is a simple spark tutorial project for begineer



### How to Get Started?

- Install VirtualBox & Vagrant
  - For Mac OS
  <pre><code>
  brew cask install virtualbox
  brew cask install vagrant
  </code><pre>
  
- Download PySpark Mooc Environment
  
  - `wget https://github.com/spark-mooc/mooc-setup/archive/master.zip`
  
- Unzip and cd mooc-setup-master/
  
- vagrant up (boot-up PySpark virtual machine)
  
- vagrant ssh
  
- Install additional packages
  
  ``` shell
sudo apt-get update
sudo apt-get install git
git clone https://github.com/texib/spark_tutorial.git
sudo apt-get install libxml2-dev libxslt1-dev python-dev
sudo apt-get install python-lxml
sudo pip install BeautifulSoup4
sudo pip install jieba
sudo pip install wordcloud
sudo apt-get install python-imaging
wget https://github.com/l10n-tw/cwtex-q-fonts-TTFs/raw/master/ttf/cwTeXQFangsong-Medium.ttf
sudo apt-get install python-numpy python-scipy
sudo pip install gensim
sudo apt-get python-matplotlib
sudo pip uninstall numpy
sudo pip install numpy == 1.9.2 #Bug in MLlib, need version 1.9.2
  ```
- If `pip` command has some problems amount this steps
  
  ``` shell
  sudo apt-get remove python-pip
  sudo easy_install pip
  sudo ln -s /usr/local/bin/pip /usr/bin/pip
  ```
  
- Open IPython Notebook in browser — `http://localhost:8001/tree`
  
- Follow the steps from the slides below.



### Slides Link

https://docs.google.com/presentation/d/1hFpHcIANEyb2RtdyJboxVtPoyfHj_9wUWXby9dYZf9I/edit?usp=sharing



### MOOK Resource

https://github.com/spark-mooc



### IPython Notebook

> IPython Notebooks can integrate formatted text (Markdown), executable code (Python), mathematical formulae (LaTeX), and graphics/visualizations ([matplotlib](http://matplotlib.org/)) into a single document that captures the flow of an exploration and can be exported as a formatted report or an executable script. — [[Link](http://blog.cloudera.com/blog/2014/08/how-to-use-ipython-notebook-with-apache-spark/)]

1. BASIC RDD Operation
   - practice simple map, reduce, reduceByKey to count number
2. ProcessText Data
   - **requests**, **urllib2** read web page
   - json load, encode
   - Write ***word_count*** for an article set
3. AnalysisArticle_HTML
   - Practice HTML related tools
     - **urlparse**, **lxml.html**, **xpath**
   - Write code to sort img source netloc in several articles

4. AnalysisArticle_Content
   - Practice use BeautifulSoup to parse webpage
   - Practice use Jieba to splite Chinese word
   - Try to print significant word in WordCloud

5. Classification - Article_Content
   - Use BeautifulSoup and Jieba to do article preprocess
   - First know about MLlib with SparseVector and LabeledPoint
   - Try MLlib NaiveBayes to build a simple article type classifier


### How to set up IPython Notebook to work smoothly with PySpark?

> Cloudera Hos-to Doc: http://blog.cloudera.com/blog/2014/08/how-to-use-ipython-notebook-with-apache-spark/

***Environment setting***

- ipython profile create pyspark
  
- Edit `~/.ipython/profile_pyspark/ipython_notebook_config.py` to have
  
  ``` shell
  c = get_config()
  c.NotebookApp.ip = '*'
  c.NotebookApp.port = 8001 # or whatever you want
  ```
  
  - If you run PySpark in vagrant vm, please make sure this port is sync to forwarding port in Vagrantfile.
  
- Create file `~/.ipython/profile_pyspark/startup/00-pyspark-setup.py` with the following contents.
  
  ``` shell
  import os
  import sys
   
  spark_home = os.environ.get('SPARK_HOME', None)
    if not spark_home:
        raise ValueError('SPARK_HOME environment variable is not set')
  sys.path.insert(0, os.path.join(spark_home, 'python'))
    sys.path.insert(0, os.path.join(spark_home, 'python/lib/py4j-0.8.1-src.zip'))
  execfile(os.path.join(spark_home, 'python/pyspark/shell.py'))
  ```
  
- Starting IPython Notebook with PySpark
  
  - `/usr/bin/python /usr/local/bin/ipython notebook --profile=pyspark`

***Set-up inside IPython Notebook***

- Input the following script to IPython Shell
  
  ``` shell
  import os
  import sys
  
  spark_home = os.environ.get('SPARK_HOME', None)
  if not spark_home:
      raise ValueError('SPARK_HOME environment variable is not set')
  sys.path.insert(0, os.path.join(spark_home, 'python'))
  sys.path.insert(0, os.path.join(spark_home, 'python/lib/py4j-0.8.2.1-src.zip'))
  
  print sys.path
  execfile(os.path.join(spark_home, 'python/pyspark/shell.py'))
  ```
