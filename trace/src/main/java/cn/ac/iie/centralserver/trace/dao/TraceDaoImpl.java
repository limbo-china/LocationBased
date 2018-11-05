package cn.ac.iie.centralserver.trace.dao;

import java.util.List;

import org.hibernate.criterion.DetachedCriteria;
import org.hibernate.criterion.Restrictions;
import org.springframework.orm.hibernate5.support.HibernateDaoSupport;

import cn.ac.iie.centralserver.trace.bean.TraceDBData;

public class TraceDaoImpl extends HibernateDaoSupport implements TraceDao {

	@Override
	public List<TraceDBData> getDBData(String queryType, String index,
			String starttime, String endtime) {
		DetachedCriteria dc = DetachedCriteria.forClass(TraceDBData.class);

		dc.add(Restrictions.eq("c_" + queryType, index));

		List<TraceDBData> list = (List<TraceDBData>) getHibernateTemplate()
				.findByCriteria(dc);

		return list;
	}

}