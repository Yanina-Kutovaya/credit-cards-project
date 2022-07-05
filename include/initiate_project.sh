astro dev init
git init
git remote add origin git@github.com:Yanina-Kutovaya/credit-cards-project.git
git branch -M main
git push -uf origin main

kedro new
cp -r credit-cards-project/* .
rm -r credit-cards-project

pip install dvc[all]
dvc init
git commit -m "Initialize DVC"
dvc remote add -d s3store s3://credit-cards-data/01_raw
dvc remote modify s3store endpointurl https://storage.yandexcloud.net

git rm -r --cached 'data'
git commit -m "stop tracking data"
git push

dvc add data 
git add data.dvc
dvc config core.autostage true
dvc install
git push

dvc add data        # after any chages in 'data' folder before commit
dvc pull            # after git pull to get changes from remote

dvc remove data.dvc # to stop data tracking

