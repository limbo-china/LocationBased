package cn.ac.iie.centralserver.trace.dao;

import java.util.List;

import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;

import cn.ac.iie.centralserver.trace.bean.TraceDBData;

public class TraceDaoImpl implements TraceDao {

	Configuration configuration = new Configuration().configure();
	SessionFactory sessionFactory = configuration.buildSessionFactory();

	@Override
	public List<TraceDBData> getDBData(String queryType, String index,
			String starttime, String endtime) {
		Session session = sessionFactory.openSession();

		Query query = session.createQuery("from TraceDBData where c_"
				+ queryType + "= ?");
		query.setParameter(0, index);
		List<TraceDBData> list = query.list();

		session.close();

		return list;
	}

}