SNOWFLAKE_CONNECTION ?= DQ-APP
ACCOUNT 			 ?= TKNLTGA-XP02744
ROLE                 ?= CDM_ELT
WAREHOUSE            ?= CDM_ELT_WH
DATABASE             ?= PCORNET_CDM
SCHEMA 			 	 ?= DQ_SCHEMA	
COMPUTE_POOL         ?= DQ_COMPUTE_POOL
APP_NAME             ?= PCORNET_DQ_DASHBOARD

.PHONY: deploy update url


local: ## [DEPLOY] Run the app locally (opens browser for SSO, then Streamlit)
	@echo "── Starting local Streamlit server (DQ_APP_MODE=local) ──"
	DQ_APP_MODE=local streamlit run streamlit_app.py

deploy:  
	@echo "── Deploying $(APP_NAME) on compute pool $(COMPUTE_POOL) ──"
	@snow streamlit deploy --open -c $(SNOWFLAKE_CONNECTION)
	@echo "  ✅ Deployed"

update:
	@echo "── Updating $(APP_NAME) ──"
	@snow streamlit deploy --replace -c $(SNOWFLAKE_CONNECTION)
	@echo "  ✅ Updated"

url: ## [DEPLOY] Print the app URL
	@snow streamlit get-url $(APP_NAME) -c $(SNOWFLAKE_CONNECTION) 2>/dev/null || \
		echo "App not found. Run 'make deploy' first."
