input_sample = pd.DataFrame({"accountNumber": pd.Series(["1201327"], dtype="int64"), "institution": pd.Series(["837"], dtype="int64"), "transit": pd.Series(["96580"], dtype="int64"), "date": pd.Series(["2020-08-09T00:00:00.000Z"], dtype="datetime64[ns]"), "transaction": pd.Series(["MB-Transfer to account"], dtype="object"), "transactionType": pd.Series(["Withdrawal"], dtype="object"), "openingBalance": pd.Series(["101490.475519844"], dtype="float64"), "amountsWithdrawn": pd.Series(["-1059.0743587818"], dtype="float64"), "amountsDeposited": pd.Series(["0.0"], dtype="float64"), "balance": pd.Series(["81471.7739857773"], dtype="float64"), "mlIdentifier": pd.Series([], dtype="object"), "meanTranscPerDisctAccntPerMonth": pd.Series(["13.0"], dtype="float64"), "sixmonthTotalamountsDeposited_OA": pd.Series(["10057.0"], dtype="float64"), "sixmonthTotalamountsWithdrawn_OA": pd.Series(["1059.0"], dtype="float64"), "threemonthTotalamountsDeposited_OA": pd.Series(["5251.0"], dtype="float64"), "threemonthTotalamountsWithdrawn_OA": pd.Series(["1059.0"], dtype="float64"), "sixmonthTotalamountsDeposited_MA": pd.Series(["198004.0"], dtype="float64"), "sixmonthTotalamountsWithdrawn_MA": pd.Series(["143706.0"], dtype="float64"), "threemonthTotalamountsDeposited_MA": pd.Series(["113871.0"], dtype="float64"), "threemonthTotalamountsWithdrawn_MA": pd.Series(["71053.0"], dtype="float64"), "frequentDepositSum": pd.Series(["11804.0"], dtype="float64"), "sixmonthsagoaccountDetailsgroupDepositeddata": pd.Series(["1317957.0"], dtype="float64"), "sixmonthsagoaccountDetailsgroupWithdrawndata": pd.Series(["459240.0"], dtype="float64"), "threemonthsagoaccountDetailsgroupDepositeddata": pd.Series(["659699.0"], dtype="float64"), "threemonthsagoaccountDetailsgroupWithdrawndata": pd.Series(["234985.0"], dtype="float64"), "flaggedAccntVal": pd.Series(["0.0"], dtype="float64"), "meanTranscPerDisctAccntPerMonth_Rank": pd.Series(["0.0"], dtype="float64"), "sixmonthTotalamountsDeposited_OA_Rank": pd.Series(["0.0"], dtype="float64"), "sixmonthTotalamountsWithdrawn_OA_Rank": pd.Series(["0.0"], dtype="float64"), "threemonthTotalamountsDeposited_OA_Rank": pd.Series(["5.0"], dtype="float64"), "threemonthTotalamountsWithdrawn_OA_Rank": pd.Series(["0.0"], dtype="float64"), "sixmonthTotalamountsDeposited_MA_Rank": pd.Series(["0.0"], dtype="float64"), "sixmonthTotalamountsWithdrawn_MA_Rank": pd.Series(["0.0"], dtype="float64"), "threemonthTotalamountsDeposited_MA_Rank": pd.Series(["0.0"], dtype="float64"), "threemonthTotalamountsWithdrawn_MA_Rank": pd.Series(["1.0"], dtype="float64"), "frequentDepositSum_Rank": pd.Series(["1.0"], dtype="float64"), "sixmonthsagoaccountDetailsgroupDepositeddata_Rank": pd.Series(["0.0"], dtype="float64"), "sixmonthsagoaccountDetailsgroupWithdrawndata_Rank": pd.Series(["0.0"], dtype="float64"), "threemonthsagoaccountDetailsgroupDepositeddata_Rank": pd.Series(["0.0"], dtype="float64"), "threemonthsagoaccountDetailsgroupWithdrawndata_Rank": pd.Series(["0.0"], dtype="float64"), "flaggedAccntVal_Rank": pd.Series(["1.0"], dtype="float64"), "accountDetails": pd.Series(["2971184"], dtype="int64")})
output_sample = np.array([0])
try:
    log_server.enable_telemetry(INSTRUMENTATION_KEY)
    log_server.set_verbosity('INFO')
    logger = logging.getLogger('azureml.automl.core.scoring_script')
except:
    pass


def init():
    global model
    # This name is model.id of model that we want to deploy deserialize the model file back
    # into a sklearn model
    model_path = os.path.join(os.getenv('AZUREML_MODEL_DIR'), 'model.pkl')
    try:
        model = joblib.load(model_path)
    except Exception as e:
        path = os.path.normpath(model_path)
        path_split = path.split(os.sep)
        log_server.update_custom_dimensions({'model_name': path_split[1], 'model_version': path_split[2]})
        logging_utilities.log_traceback(e, logger)
        raise


@input_schema('data', PandasParameterType(input_sample))
@output_schema(NumpyParameterType(output_sample))
def run(data):
    try:
        result = model.predict(data)
        return json.dumps({"result": result.tolist()})
    except Exception as e:
        result = str(e)
        return json.dumps({"error": result})
