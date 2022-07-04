astro dev init
git init
git remote add origin https://gitlab.com/Yanina_K/credit-cards-project.git
git branch -M main
git push -uf origin main

kedro new
cp -r credit-cards-project/* .
rm -r credit-cards-project
