import os
from datetime import datetime

current_time = datetime.now().strftime("%H:%M:%S")
rdir = r'C:\Users\rmagala\OneDrive\pyapsimx'
os.chdir(rdir)
os.system("git init")
os.system("git remote rm origin")
os.system("git remote add origin https://github.com/MAGALA-RICHARD/pyapsimx.git")
os.system('git config --global user.email "rmagala640@gmail.com"')
os.system('git config --global user.name "RICHARD-MAGALA"')
os.system("git rm -r --cached .")
os.system('git add .')
git_commit_with_time = f'git commit -m "update:{current_time}"'
os.system(git_commit_with_time)
os.system("git push -f --set-upstream origin master")
