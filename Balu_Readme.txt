create Virtual environment
	pip install uv
	uv inti
	uv sync # it will create virtual environment autometically
	.venv\Scripts\activate
	if you are facing in any issue with above command run the below command in powershell (windows)
		Set-ExecutionPolicy -Scope CurrentUser -ExecutionPolicy RemoteSigned
	uv add apache-airflow # adding depedencies	

create .env file in root folder 
add this "AIRFLOW_UID=50000" in .env file