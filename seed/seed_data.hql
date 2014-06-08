use hadoop;


dfs -copyFromLocal vehicle_data.csv '/user/hadoop/data/vehicle';
dfs -copyFromLocal usage_data.csv '/user/hadoop/data/usage';
