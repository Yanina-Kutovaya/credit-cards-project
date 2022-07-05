astro dev init
git init
git remote add origin git@github.com:Yanina-Kutovaya/credit-cards-project.git
git branch -M main
git push -uf origin main

kedro new
cp -r credit-cards-project/* .
rm -r credit-cards-project
