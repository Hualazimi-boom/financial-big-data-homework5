"""Independent pandas/numpy checks, deterministic holdout, no sklearn needed."""
from pathlib import Path
import sys,json,hashlib
import numpy as np
import pandas as pd

ROOT=Path(__file__).resolve().parent
DATA=Path(sys.argv[1]) if len(sys.argv)>1 else Path('D:/课程/金融大数据/实验/实验2')
a=pd.read_csv(DATA/'train.csv'); b=pd.read_csv(DATA/'test.csv')
money=[c for c in a if c.endswith('_AMT') or c=='G_OS_PRCP_SUM']
a[money]=a[money].fillna(0)
cents=lambda s: np.rint(s.to_numpy()*100).astype(np.int64)
asset=cents(a.CUST_ASSET_AMT); debt=cents(a.CUST_DEBT_AMT)
q=np.sort(asset)[np.ceil(np.array([.25,.5,.75])*len(a)).astype(int)-1]
tiers=np.searchsorted(q,asset,side='left')
stats={'train_n':len(a),'test_n':len(b),'org_n':int(a.OPEN_ORG_NUM.nunique()),'label_counts':{str(k):int(v) for k,v in a.bad_good.value_counts().items()},'q':(q/100).tolist(),'asset_sum':int(asset.sum())/100,'debt_sum':int(debt.sum())/100,'duplicates_train':int(a.CUST_ID.duplicated().sum()),'duplicates_test':int(b.CUST_ID.duplicated().sum()),'overlap_ids':len(set(a.CUST_ID)&set(b.CUST_ID)),'missing_train':int(a.isna().sum().sum()),'missing_test':int(b.isna().sum().sum()),'negative_money':{c:int((a[c]<0).sum()) for c in money},'tier_counts':np.bincount(tiers,minlength=4).tolist(),'asset_component_mismatch':int((np.rint(a[money[:7]].sum(axis=1).to_numpy()*100).astype(np.int64)!=asset).sum()),'sha256':{x:hashlib.sha256((DATA/x).read_bytes()).hexdigest() for x in ['train.csv','test.csv']}}
(ROOT/'validation').mkdir(exist_ok=True)
rng=np.random.default_rng(20261102); valid=[]
for label in [0,1]:
    ix=np.flatnonzero(a.bad_good.to_numpy()==label);rng.shuffle(ix);valid.extend(ix[:round(.2*len(ix))])
mask=np.zeros(len(a),dtype=bool);mask[valid]=True
a[~mask].to_csv(ROOT/'validation/train.csv',index=False)
a[mask].drop(columns='bad_good').to_csv(ROOT/'validation/holdout.csv',index=False)
a.loc[mask,['CUST_ID','bad_good']].to_csv(ROOT/'validation/truth.csv',index=False)
qv=np.sort(asset[~mask])[np.ceil(np.array([.25,.5,.75])*(~mask).sum()).astype(int)-1]
stats['validation_q']=(qv/100).tolist();stats['validation_train_n']=int((~mask).sum());stats['validation_n']=int(mask.sum())
stats['top_orgs']=a.assign(asset_cents=asset,debt_cents=debt).groupby('OPEN_ORG_NUM').agg(n=('CUST_ID','size'),asset=('asset_cents','sum'),debt=('debt_cents','sum'),bad=('bad_good','sum')).sort_values('n',ascending=False).head(10).reset_index().to_dict('records')
stats['structures']=[]
for t in range(4):
    sums=np.array([cents(a.loc[tiers==t,c]).sum() for c in money[:7]],dtype=np.int64)
    stats['structures'].append({'sums':sums.tolist(),'shares':(sums/sums.sum()).tolist(),'bad_rate':float(a.loc[tiers==t,'bad_good'].mean())})
(ROOT/'summary.json').write_text(json.dumps(stats,ensure_ascii=False,indent=2),encoding='utf-8')
print(json.dumps(stats,ensure_ascii=False,indent=2))

if (ROOT/'output/task1').exists():
    def lines(d):
        return [l for f in sorted((ROOT/d).glob('part-*')) for l in f.read_text(encoding='utf-8').splitlines()]
    t1={l.split('\t')[0]:l.split('\t')[1].split(',') for l in lines('output/task1')}
    t2={l.split('\t')[0]:list(map(int,l.split('\t')[1].split(','))) for l in lines('output/task2')}
    for org,g in a.groupby('OPEN_ORG_NUM'):
        av=cents(g.CUST_ASSET_AMT);dv=cents(g.CUST_DEBT_AMT);z=t1[str(org)]
        assert [int(z[0]),round(float(z[1])*100),round(float(z[2])*100),int(z[3])]==[len(g),int(av.sum()),int(dv.sum()),int(g.bad_good.sum())]
        high=sum(int(x)*len(g)>=int(av.sum()) for x in av)
        assert t2[str(org)]==[high,len(g)-high]
    assert lines('output/task3')==['\t'.join(f'{k}:{v/100:.2f}' for k,v in zip(['Q25','Q50','Q75'],q))]
    names=['存款','理财','私人理财','基金','国债','银保通','贵金属'];tn=['低资产','中等资产','较高资产','高资产']
    expected=[]
    for t in range(4):
        shares=stats['structures'][t]['shares']
        expected.extend((tn[t],names[i],shares[i]) for i in sorted(range(7),key=lambda i:-shares[i]))
    actual=[l.split('\t') for l in lines('output/task4')]
    assert len(actual)==28
    for z,e in zip(actual,expected):assert tuple(z[:2])==e[:2] and abs(float(z[2])-e[2])<5.1e-9
    pred=pd.concat([pd.read_csv(f,sep='\t',header=None,names=['CUST_ID','prediction']) for f in sorted((ROOT/'output/task5').glob('part-*'))],ignore_index=True)
    assert len(pred)==len(b) and pred.CUST_ID.is_unique and set(pred.CUST_ID)==set(b.CUST_ID)
    assert set(pred.prediction)<= {0,1}
    stats['test_predictions']={str(k):int(v) for k,v in pred.prediction.value_counts().items()}
    (ROOT/'checks.txt').write_text('PASS task1: every institution/count/asset/debt/label\nPASS task2: every institution including ties\nPASS task3: all nearest-rank quantiles\nPASS task4: all 28 rows and sorting\nPASS task5: full coverage, unique IDs, binary labels\n',encoding='utf-8')
    (ROOT/'summary.json').write_text(json.dumps(stats,ensure_ascii=False,indent=2),encoding='utf-8')
    print('ALL INDEPENDENT CHECKS PASSED')
