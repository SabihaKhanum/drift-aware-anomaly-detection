# drift-aware-anomaly-detection
end-to-end adaptive anomaly detection architecture for financial and IoT time-series streams, built natively inside Apache Flink. 

$env:ADAPTIVE_MODE="false"; python adaptivedetector_v2.py
$env:ADAPTIVE_MODE="true"; python adaptivedetector_v2.py 
python .\coindcx_producer.py      
 streamlit run dashboard.py        
pip install river scikit-learn numpy --break-system-packages
python3 synthetic_drift_benchmark.py