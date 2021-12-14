- To check if airflow is installed just run:
    - Command: 'airflow list_dags'
    - If you get an output for the above command then well and good. Go ahead and check the version of airflow that is running
    - Command: 'airflow version'

- Once verified, try to compile the code for our dag, i.e. pdill_no_op_2.1.0.py
    - Command: 'python pdill_no_op_2.1.0.py'
    - Although I have tested it in my machine and the imports look good to me, please compile the code in the target environment.
    - Mostly you can expect import errors when there are version differences. You can just google and find out the right import. 
        - If you are not able to resolve it, tell me the import error and the version of airflow I can try to look into the source code and work it out for you

- If the version is 2.1.0 and still there are import errors for SimpleHttpOperator run the following command.
    - Command: 'pip install apache-airflow-providers-http==2.0.1'
    - this should resolve it