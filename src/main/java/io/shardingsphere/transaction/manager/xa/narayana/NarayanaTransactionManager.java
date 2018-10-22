/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2018, Red Hat, Inc., and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package io.shardingsphere.transaction.manager.xa.narayana;

import com.arjuna.ats.internal.jta.recovery.arjunacore.XARecoveryModule;
import com.arjuna.ats.jta.common.jtaPropertyManager;
import com.arjuna.ats.jta.recovery.XAResourceRecoveryHelper;
import io.shardingsphere.core.event.transaction.xa.XATransactionEvent;
import io.shardingsphere.core.rule.DataSourceParameter;
import io.shardingsphere.transaction.manager.xa.XATransactionManager;

import javax.sql.DataSource;
import javax.sql.XADataSource;
import javax.transaction.UserTransaction;
import java.sql.SQLException;

/**
 * @author zhfeng
 */
public class NarayanaTransactionManager implements XATransactionManager {
    private static final UserTransaction USER_TRANSACTION_MANAGER = jtaPropertyManager.getJTAEnvironmentBean().getUserTransaction();
    private static final XARecoveryModule xaRecoveryModule =  XARecoveryModule.getRegisteredXARecoveryModule();

    @Override
    public DataSource wrapDataSource(XADataSource dataSource, String dataSourceName, DataSourceParameter dataSourceParameter) throws Exception {
        DataSource ds = new NarayanaDataSource(dataSource);
        xaRecoveryModule.addXAResourceRecoveryHelper(getRecoveryHelper(dataSource, dataSourceParameter));
        return ds;
    }

    private XAResourceRecoveryHelper getRecoveryHelper(XADataSource xaDataSource, DataSourceParameter dataSourceParameter) {
        String username = dataSourceParameter.getUsername();
        String password = dataSourceParameter.getPassword();

        if (username != null && password != null) {
            return new DataSourceXAResourceRecoveryHelper(xaDataSource, username, password);
        } else {
            return new DataSourceXAResourceRecoveryHelper(xaDataSource);
        }

    }

    @Override
    public void begin(XATransactionEvent transactionEvent) throws SQLException {
        try {
            USER_TRANSACTION_MANAGER.begin();
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    @Override
    public void commit(XATransactionEvent transactionEvent) throws SQLException {
        try {
            USER_TRANSACTION_MANAGER.commit();
        } catch (Exception e) {
            throw new SQLException(e);
        }

    }

    @Override
    public void rollback(XATransactionEvent transactionEvent) throws SQLException {
        try {
            USER_TRANSACTION_MANAGER.rollback();
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    @Override
    public int getStatus() throws SQLException {
        try {
            return USER_TRANSACTION_MANAGER.getStatus();
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }
}
