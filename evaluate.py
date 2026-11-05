"""Evaluate only held-out labels; independently reproduce Java NB probabilities."""
from pathlib import Path
import json,sys
import numpy as np
import pandas as pd
R=Path(__file__).resolve().parent
s=json.loads((R/'summary.json').read_text(encoding='utf-8'))
v=R/'validation'
truth=pd.read_csv(v/'truth.csv')
p=pd.concat([pd.read_csv(f,sep='\t',header=None,names=['CUST_ID','prediction','probability']) for f in sorted((v/'predictions').glob('part-*'))])
assert len(p)==len(truth) and p.CUST_ID.is_unique
d=truth.merge(p,on='CUST_ID',validate='one_to_one');assert len(d)==len(truth)
y=d.bad_good.to_numpy(); yh=d.prediction.to_numpy();prob=d.probability.to_numpy()
tp=int(((y==1)&(yh==1)).sum());tn=int(((y==0)&(yh==0)).sum());fp=int(((y==0)&(yh==1)).sum());fn=int(((y==1)&(yh==0)).sum())
ranks=pd.Series(prob).rank(method='average').to_numpy();n1=int(y.sum());n0=len(y)-n1
auc=float((ranks[y==1].sum()-n1*(n1+1)/2)/(n1*n0))
order=np.argsort(-prob,kind='stable');sy=y[order];sp=prob[order];ends=np.r_[np.flatnonzero(np.diff(sp)),len(sp)-1];cum=np.cumsum(sy);rec=cum[ends]/n1;precision=cum[ends]/(ends+1);ap=float(np.sum(np.diff(np.r_[0,rec])*precision))
metrics={'n':len(y),'positive':n1,'negative':n0,'threshold':.5,'TP':tp,'TN':tn,'FP':fp,'FN':fn,'accuracy':(tp+tn)/len(y),'precision':tp/(tp+fp) if tp+fp else 0,'recall':tp/(tp+fn),'f1':2*tp/(2*tp+fp+fn),'balanced_accuracy':.5*(tp/(tp+fn)+tn/(tn+fp)),'roc_auc':auc,'average_precision':ap,'majority_accuracy':n0/len(y),'majority_f1':0,'majority_roc_auc':.5,'majority_average_precision':n1/len(y),'positive_rate':n1/len(y)}
# Rebuild scores independently from the MapReduce-produced sufficient statistics.
money=['CUST_SAVING_AMT','CUST_FINA_AMT','CUST_PRIFINA_AMT','CUST_FOND_AMT','CUST_NADEBT_AMT','CUST_YBT_AMT','CUST_METAL_AMT','CUST_ASSET_AMT','CUST_DEBT_AMT','G_OS_PRCP_SUM']
def check_scores(frame,model_dir,q,pred,detail):
 model={};vocab={i:set() for i in range(14)}
 for f in model_dir.glob('part-*'):
  for line in f.read_text(encoding='utf-8').splitlines():
   k,c=line.split('\t');model[k]=int(c);z=k.split('|')
   if z[1]!='N':vocab[int(z[1])].add(z[2])
 n=[model[f'{i}|N'] for i in [0,1]]
 features=[]
 for col in money:
  x=frame[col].fillna(0).to_numpy();bins=np.minimum(30,np.floor(np.log10(1+np.maximum(x,0))*2)).astype(int)
  features.append(np.where(x<0,'NEG',np.where(x==0,'ZERO',np.char.add('B',bins.astype(str)))))
 for col in ['OPEN_ORG_NUM','LOAN_FLAG','CRED_FLAG']:features.append(frame[col].fillna('').astype(str).to_numpy())
 features.append(np.searchsorted(np.rint(np.array(q)*100).astype(np.int64),np.rint(frame.CUST_ASSET_AMT.to_numpy()*100).astype(np.int64),side='left').astype(str))
 scores=[]
 for label in [0,1]:
  score=np.full(len(frame),np.log((n[label]+1)/(sum(n)+2)))
  for i,f in enumerate(features):
   score+=np.log(np.array([model.get(f'{label}|{i}|{x}',0)+1 for x in f])/(n[label]+len(vocab[i])+1))
  scores.append(score)
 score=1/(1+np.exp(np.clip(scores[0]-scores[1],-700,700)))
 independent=pd.DataFrame({'CUST_ID':frame.CUST_ID,'score':score})
 joined=independent.merge(pred,on='CUST_ID',validate='one_to_one');assert len(joined)==len(frame)
 assert np.array_equal((joined.score>=.5).astype(int),joined.prediction)
 if detail:assert np.max(np.abs(joined.score-joined.probability))<1e-12
 return float(score.mean())
metrics['independent_score_check']=check_scores(pd.read_csv(v/'holdout.csv'),v/'fitted/model',s['validation_q'],p,True)
data=Path(sys.argv[1]) if len(sys.argv)>1 else Path('D:/课程/金融大数据/实验/实验2')
metrics['full_model_mean_score']=check_scores(pd.read_csv(data/'test.csv'),R/'output/model',s['q'],pd.concat([pd.read_csv(f,sep='\t',header=None,names=['CUST_ID','prediction']) for f in (R/'output/task5').glob('part-*')]),False)
(R/'metrics.json').write_text(json.dumps(metrics,indent=2),encoding='utf-8')
with (R/'checks.txt').open('a',encoding='utf-8') as f:f.write('PASS validation NB: all probabilities match independent NumPy implementation within 1e-12\nPASS test NB: all predicted labels match independent NumPy implementation\n')
# Curve points for compact pgfplots drawings.
idx=np.unique(np.linspace(0,len(ends)-1,150).astype(int));fp_curve=(ends+1-cum[ends])/n0
pd.DataFrame({'fpr':np.r_[0,fp_curve[idx]],'tpr':np.r_[0,rec[idx]]}).to_csv(R/'report/roc.csv',index=False)
print(json.dumps(metrics,indent=2))
