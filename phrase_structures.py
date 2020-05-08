import copy
from utils import *


# built-in functions
# THE RESULTS OF DATA AGGREGATION SHOULD BE VALUENP?
# data aggregation
def _avg(x):
	assert (isinstance(x, PROPERTYNP))
	# assert (x.dtype in CALCULABLE_DTYPES)
	# the property to be averaged must be single
	c_english = 'the average of %s' % x.c_english
	c_chinese = '%s的平均值' % x.c_chinese
	z = ' AVG(%s) ' % x.z
	# z4toks = 'AVG ( %s ) ' % x.z
	return c_english, c_chinese, z, False


def _min(x):
	assert (isinstance(x, PROPERTYNP))
	# assert (x.dtype in SUBTRACTABLE_DTYPES)
	c_english = 'the smallest %s' % x.c_english
	c_chinese = '%s的最小值' % x.c_chinese
	z = ' MIN(%s) ' % x.z
	# z4toks = 'MIN ( %s )' % x.z
	return c_english, c_chinese, z, False


def _max(x):
	assert (isinstance(x, PROPERTYNP))
	# assert (x.dtype in SUBTRACTABLE_DTYPES)
	c_english = 'the largest %s' % x.c_english
	c_chinese = '%s的最大值' % x.c_chinese
	z = ' MAX(%s) ' % x.z
	# z4toks = 'MAX ( %s )' % x.z
	return c_english, c_chinese, z, False


def _sum(x):
	assert (isinstance(x, PROPERTYNP))
	# assert (x.dtype in CALCULABLE_DTYPES)
	c_english = 'the sum of %s' % x.c_english
	c_chinese = '%s的总和' % x.c_chinese
	z = ' SUM(%s) ' % x.z
	# z4toks = 'SUM ( %s )' % x.z
	return c_english, c_chinese, z, False


def _uniquecount(x):
	assert (isinstance(x, PROPERTYNP))

	# the count must be of integer type, specifically ordinal integer type
	rho = random.random()
	if rho < 0.4 and x.dtype != 'star':
		distinct = True
		c_english = 'the number of different values of %s' % x.c_english
		c_chinese = '%s不同取值的个数' % x.c_chinese
		z = ' COUNT(distinct %s) ' % x.z
	# z4toks = 'COUNT ( distinct %s )' % x.z
	else:
		distinct = False
		if x.dtype == 'star':
			c_english = 'the number of entries'
			c_chinese = '个数'
		else:
			c_english = 'the number of %s' % x.c_english
			c_chinese = '%s的个数' % x.c_chinese
		z = ' COUNT(%s) ' % x.z
	# z4toks = 'COUNT ( %s )' % x.z
	return c_english, c_chinese, z, distinct


# for use in converting an SQL entry into a 'NP' class object
def _count_uniqueness_specified(x, distinct):
	assert (isinstance(x, PROPERTYNP))
	# the count must be of integer type, specifically ordinal integer type
	if distinct and x.dtype != 'star':
		c_english = 'the number of different values of %s' % x.c_english
		c_chinese = '%s不同取值的个数' % x.c_chinese
		z = ' COUNT(distinct %s) ' % x.z
	# z4toks = 'COUNT ( distinct %s )' % x.z
	else:
		if x.dtype == 'star':
			c_english = 'the number of entries'
			c_chinese = '个数'
		else:
			c_english = 'the number of %s' % x.c_english
			c_chinese = '%s的个数' % x.c_chinese
		z = ' COUNT(%s) ' % x.z
	# z4toks = 'COUNT ( %s )' % x.z
	return c_english, c_chinese, z, distinct


def _repeat(x):
	return x.c_english, x.c_chinese, x.z, False


# set aggregator number 0 as returning the same as what's fed in
AGGREGATION_FUNCTIONS = [_repeat, _max, _min, _uniquecount, _sum, _avg]


class PROP_REL():
	def __init__(self, prop_source, prop_target, type_source, type_target, score):
		self.prop_ids = [prop_source, prop_target]
		self.table_ids = [type_source, type_target]
		self.score = score

	def print(self):
		print("----")
		print("prop_ids: ", self.prop_ids)
		print("table_ids: ", self.table_ids)
		print("score: ", self.score)
		print("~~~~")


class BASENP():
	# dtypes:
	#	self:			----
	#	c_english:		string
	#	c_chinese:		string
	#	z:				string
	def __init__(self, c_english, c_chinese, z):
		self.c_english = c_english
		self.c_chinese = c_chinese
		self.z = z


# TYPENP & NP has attributes; VALUENP, PROPERTYNP & CDT requires properties
# dtypes here down below are associated with 'attributes' like things, not 'property_names' like things

class TYPENP(BASENP):
	# dtypes:
	#	self:			----
	#	c_english:		string
	#	c_chinese:		string
	#	z:				string
	#	overall_idx:	int
	#	properties:		list		# contains the indices of its properties
	#	valid:			bool		# whether there are data entries in this table
	def __init__(self, c_english, c_chinese, z, overall_idx=None, properties=None):
		BASENP.__init__(self, c_english, c_chinese, z)
		if properties is None:
			properties = []
		self.overall_idx = overall_idx
		self.properties = properties
		self.valid = True
		self.clones = []
		self.is_edge = False

	def print(self):
		print("c_english: ", self.c_english)
		print("c_chinese: ", self.c_chinese)
		print("z: ", self.z)
		print("overall_idx: ", self.overall_idx)
		print("properties: ", self.properties)


class VALUENP(BASENP):
	# dtype here should strip off the '<type= >' thing
	# dtypes does not have same length as property_names,
	# it represents the dtypes of the resulting outputs of
	# the value required, not its ingradients.
	#
	# dtypes:
	#	self:			----
	#	c_english:		string
	#	c_chinese:		string
	#	z:				string
	#	dtype:			string
	def __init__(self, c_english, c_chinese, z, dtype):
		BASENP.__init__(self, c_english, c_chinese, z)
		self.dtype = dtype
		if 'varchar(1000)' in self.dtype or 'datetime' in self.dtype:
			assert isinstance(self.z, str)
			self.c_english = self.c_english.replace('%', '')
			self.c_chinese = self.c_chinese.replace('%', '')

			if self.z[0] != '"':
				self.z = '"' + self.z
			if self.z[-1] != '"':
				self.z = self.z + '"'
			if self.c_english[0] != '"':
				self.c_english = '"' + self.c_english
			if self.c_english[-1] != '"':
				self.c_english = self.c_english + '"'
			if self.c_chinese[0] != '"':
				self.c_chinese = '"' + self.c_chinese
			if self.c_chinese[-1] != '"':
				self.c_chinese = self.c_chinese + '"'
		elif 'int' in self.dtype or 'double' in self.dtype:
			try:
				diff = abs(int(float(self.z)) - float(self.z))
			except Exception as e:
				diff = 1000	 # set diff to a large number

			if diff < 0.00001:
				self.c_english = str(int(float(self.z)))
				self.c_chinese = str(int(float(self.z)))
				self.z = str(int(float(self.z)))

	def __lt__(self, other):
		if self.z < other.z:
			return True
		else:
			return False

	def __eq__(self, other):
		if self.z == other.z:
			return True
		else:
			return False


# only take into consideration single property, the conbination of properties are dealt with at last step
# having an aggregator does not imply that it's single-valued, because there are group by clauses in it
class PROPERTYNP(BASENP):
	# dtypes:
	#	self:			----
	#	c_english:		string
	#	c_chinese:		string
	#	z:				string
	#	dtype:			string
	#	table_id:		int
	#	overall_index:	int
	#	meta_idx:		int			# The difference between meta_idx and overall_idx is that, meta_idx are the same
	#								# for clones but overall_idx are different
	#	values:			list		# The elements within are of VALUENP types since the z can be different form ce and cc
	#	aggr:			int
	#	from_sum:		bool
	#	from_cnt:		bool
	#	is_unique:		bool		# Whether each value in this property is unique
	#	valid:			bool		# Whether there are actual values in this column
	#	clones:			list		# Contains the overall_idx of the properties which are clones of this property from
	#								# same-table fk relations
	#	distinct		bool		# whether it is to be paired with 'distinct count'
	#	is_fk_left		bool		# whether it appears in the left side of a foreign key relation (left means a reference)
	#	is_fk_right		bool		# whether it appears in the right side of a foreign key relation (right means basis)
	#	is_pk			bool		# whether it is a primary key of its table
	def __init__(self, c_english, c_chinese, z, dtype, table_id=None, overall_idx=None, values=None,
				 aggr=0, from_sum=False, from_cnt=False, is_unique=False, meta_idx=None, is_pk=False):
		BASENP.__init__(self, c_english, c_chinese, z)
		self.dtype = dtype
		self.table_id = table_id
		self.overall_idx = overall_idx
		self.meta_idx = meta_idx
		self.values = values
		if self.values is not None:
			self.group_score = calc_group_score(self.dtype, self.values)
		self.aggr = aggr
		self.from_sum = from_sum
		self.from_cnt = from_cnt
		self.is_unique = is_unique
		self.is_primary = False
		self.valid = True
		self.clones = []
		self.distinct = False  # for aggregator 'COUNT', means whether it is 'count' or 'distinct count'
		self.is_fk_left = False
		self.is_fk_right = False
		self.is_pk = is_pk

	def set_values(self, values):
		assert (values is not None)
		self.values = values
		self.group_score = calc_group_score(self.dtype, self.values)

	def set_aggr(self, aggr, function=None, distinct=None):
		if self.aggr != 0:
			print(self.aggr)
			raise AssertionError
		self.aggr = aggr
		if function is not None:
			self.c_english, self.c_chinese, self.z, self.distinct = function(self, distinct)
		else:
			self.c_english, self.c_chinese, self.z, self.distinct = AGGREGATION_FUNCTIONS[self.aggr](self)
		if self.aggr == 3:
			self.from_cnt = True
			self.values = numpy.arange(len(self.values)).tolist()
		elif self.aggr == 4:
			self.from_sum = True


class CMP(BASENP):
	# dtypes:
	#	self:			----
	#	c_english:		string
	#	c_chinese:		string
	#	z:				string
	def __init__(self, c_english, c_chinese, z, index):
		BASENP.__init__(self, c_english, c_chinese, z)
		self.index = index
		self.negative = False

	# when the comparer is either 'in' or 'like', with a probability randomly set the condition to be negative
	def set_negative(self):
		assert (self.negative is False)
		self.negative = True
		self.c_english = ' not ' + self.c_english
		self.c_chinese = '不' + self.c_chinese
		self.z = ' not ' + self.z

	def set_mode(self, mode):
		if mode == 'mid':
			self.c_english = ' contains ' + self.c_english
			self.c_chinese = '包含%s的' % self.c_chinese
		elif mode == 'head':
			self.c_english = ' starts with ' + self.c_english
			self.c_chinese = '以%s为开始的' % self.c_chinese
		elif mode == 'tail':
			self.c_english = ' ends with ' + self.c_english
			self.c_chinese = '以%s为结尾的' % self.c_chinese
		elif mode == 'equal':
			self.c_english = ' equals to ' + self.c_english
			self.c_chinese = '与%s相等的' % self.c_chinese
		else:
			# 'mode' must be among 'mid', 'head' and 'tail'
			raise AssertionError


class NP:
	# inherits all properties from prev_np if specified, then replace them with any assigned properties
	# dtypes:
	#	self:			----
	#	prev_np:		NP
	#	queried_props:	PROPERTYNP
	#	table_ids:		int
	#	join_cdts:		list(CDT)
	#	cdts:			list(CDT)
	#	cdt_linkers:	string
	#	group_props:	PROPERTYNP
	#	having_cdts:		list(CDT)
	#	orderby_props:	PROPERTYNP
	#	orderby_order:	string
	#	limit:			int
	#	np_2:			NP
	#	qrynp_2:		QRYNP
	#	has_union:		bool
	#	has_except:		bool
	#	has_intersect:	bool
	#	distinct:		bool
	#	main_tids:		list(int)
	def __init__(self, prev_np=None, queried_props=None, table_ids=None, join_cdts=None,
				 cdts=None, cdt_linkers=None, group_props=None, having_cdts=None,
				 orderby_props=None, orderby_order=None, limit=None, np_2=None, qrynp_2=None, has_union=None,
				 has_except=None, has_intersect=None, distinct=False, main_tids=None):
		if prev_np is not None:
			self.queried_props = copy.deepcopy(prev_np.queried_props)
			self.table_ids = copy.deepcopy(prev_np.table_ids)
			self.join_cdts = copy.deepcopy(join_cdts)
			self.cdts = copy.deepcopy(prev_np.cdts)
			self.cdt_linkers = copy.deepcopy(cdt_linkers)
			self.group_props = copy.deepcopy(prev_np.group_props)
			self.having_cdts = copy.deepcopy(prev_np.having_cdts)
			self.orderby_props = copy.deepcopy(prev_np.orderby_props)
			self.orderby_order = copy.deepcopy(prev_np.orderby_order)
			self.limit = copy.deepcopy(prev_np.limit)
			self.np_2 = copy.deepcopy(prev_np.np_2)
			self.qrynp_2 = copy.deepcopy(prev_np.qrynp_2)
			self.has_union = prev_np.has_union
			self.has_except = prev_np.has_except
			self.has_intersect = prev_np.has_intersect
			self.distinct = prev_np.distinct
			self.main_tids = prev_np.main_tids
		else:
			self.queried_props = None
			self.table_ids = None
			self.join_cdts = None
			self.cdts = []
			self.cdt_linkers = []
			self.group_props = None
			self.having_cdts = None
			self.orderby_props = None
			self.orderby_order = None
			self.limit = None
			self.np_2 = None
			self.qrynp_2 = None
			self.has_union = None
			self.has_except = None
			self.has_intersect = None
			self.distinct = distinct
			self.main_tids = main_tids
		if queried_props is not None:
			for prop in queried_props:
				assert (isinstance(prop, PROPERTYNP))
			self.queried_props = copy.deepcopy(queried_props)
		if table_ids is not None:
			assert (join_cdts is not None)
			assert (len(join_cdts) <= len(table_ids))
			self.table_ids = copy.deepcopy(table_ids)
			self.join_cdts = copy.deepcopy(join_cdts)
		if cdts is not None:
			for cdt in cdts:
				assert (isinstance(cdt, CDT))
			self.cdts = copy.deepcopy(cdts)
			self.cdt_linkers = copy.deepcopy(cdt_linkers)
		if cdt_linkers is not None and len(cdt_linkers) > 0:
			assert (cdts is not None)
			assert (len(cdt_linkers) + 1 == len(cdts))
		if group_props is not None:
			self.group_props = copy.deepcopy(group_props)
		if having_cdts is not None:
			self.having_cdts = copy.deepcopy(having_cdts)
		if orderby_props is not None:
			self.orderby_props = copy.deepcopy(orderby_props)
			assert (orderby_order is not None)
			self.orderby_order = copy.deepcopy(orderby_order)
		if limit is not None:
			self.limit = limit
		if np_2 is not None:
			self.np_2 = copy.deepcopy(np_2)
		if qrynp_2 is not None:
			self.qrynp_2 = copy.deepcopy(qrynp_2)
		flag = False
		if has_union is not None and has_union == True:
			flag = True
			self.has_union = has_union
		if has_except is not None and has_except == True:
			assert (flag is False)
			flag = True
			self.has_except = has_except
		if has_intersect is not None and has_intersect == True:
			assert (flag is False)
			flag = True
			self.has_intersect = has_intersect


class CDT(BASENP):
	# dtypes:
	#	self:			----
	#	c_english:		string
	#	c_chinese:		string
	#	z:				string
	#	left:			PROPERTYNP
	#	right:			PROPERTYNP / VALUENP / QRYNP
	#	cmper:			CMP
	def __init__(self, c_english, c_chinese, z, left, right, cmper):
		BASENP.__init__(self, c_english, c_chinese, z)
		assert (isinstance(left, PROPERTYNP))
		assert (isinstance(cmper, CMP))
		self.left = left
		self.right = right
		self.cmper = cmper

	def fetch_z(self, temp_tabname_bucket, typenps, propertynps, start_tpos):
		if isinstance(self.right, PROPERTYNP):
			res = self.left.z.format(temp_tabname_bucket[self.left.table_id]) + self.cmper.z.format(
				self.right.z.format(temp_tabname_bucket[self.right.table_id]))
		elif isinstance(self.right, QRYNP):
			right_z, right_ztoks, right_ztoksnovalue = self.right.process_z(typenps, propertynps, start_tpos)
			if self.left.dtype == 'star':
				res = self.left.z + self.cmper.z.format(' ( ' + right_z + ' ) ')
			else:
				res = self.left.z.format(temp_tabname_bucket[self.left.table_id]) + self.cmper.z.format(
					' ( ' + right_z + ' ) ')
		else:
			# in cv conditions, there might be cases where left column is star, then there need be no formatting
			if self.left.dtype == 'star':
				res = self.z
			else:
				res = self.z.format(temp_tabname_bucket[self.left.table_id])
		return res


class QRYNP:
	def __init__(self, np, typenps, propertynps, finalize_sequence=False, is_recur=False):
		self.np = np
		self.z, self.z_toks, self.z_toks_novalue = self.process_z(typenps, propertynps, 0)
		self.c_english_verbose = self.process_ce_verbose(typenps, propertynps, is_recur)
		self.c_chinese_verbose = self.process_cc_verbose(typenps, propertynps, is_recur)
		self.c_english_sequence = self.process_ce_step_by_step(typenps, propertynps, finalize=finalize_sequence)
		self.c_chinese_sequence = self.process_cc_step_by_step(typenps, propertynps, finalize=finalize_sequence)
		self.c_english = self.c_english_verbose
		self.c_chinese = self.c_chinese_verbose

	def process_z(self, typenps, propertynps, start_tpos):
		z = 'select '
		z_toks = ['select']

		if self.np.distinct:
			z += 'distinct '
			z_toks.append('distinct')

		# queried_props
		for idx, prop in enumerate(self.np.queried_props):
			if prop.dtype == 'star':
				z += prop.z.format('')
				z_toks += prop.z.format('').split()
			elif len(self.np.table_ids) > 1:
				table_pos = None
				for i, item in enumerate(self.np.table_ids):
					if item == prop.table_id:
						table_pos = i + 1
				if table_pos is None or start_tpos is None:
					print("!!!")
				table_pos += start_tpos
				z += prop.z.format('T{0}.'.format(table_pos))
				z_toks += prop.z.format('T{0} . '.format(table_pos)).split()
			else:
				z += prop.z.format('')
				z_toks += prop.z.format('').split()

			if idx < len(self.np.queried_props) - 1:
				z += ' , '
				z_toks.append(',')

		if len(self.np.queried_props) == 0:
			z += '*'
			z_toks.append('*')

		# table_ids
		z += ' from '
		z_toks.append('from')
		table_names = []
		temp_tabname_bucket = {}
		for no, tabid in enumerate(self.np.table_ids):
			if len(self.np.table_ids) == 1:
				table_names.append(typenps[tabid].z)
				temp_tabname_bucket[tabid] = ''
			else:
				table_names.append(typenps[tabid].z + ' as T%d ' % (no + 1 + start_tpos))
				temp_tabname_bucket[tabid] = 'T%d . ' % (no + 1 + start_tpos)

		z += (' ' + ' join '.join(table_names) + ' ')
		z_toks += (' ' + ' join '.join(table_names) + ' ').split(' ')

		# which index to start counting tables from when creating sub-queries
		next_start_tpos = start_tpos + len(self.np.table_ids)

		# join_cdts
		if self.np.join_cdts is not None:
			if len(self.np.join_cdts) > 0:
				z += ' on '
				z_toks.append('on')
				join_cdt_zs = []
				for cond in self.np.join_cdts:
					join_cdt_zs.append(cond.fetch_z(temp_tabname_bucket, typenps, propertynps, next_start_tpos))
				z += (' ' + ' and '.join(join_cdt_zs) + ' ')
				z_toks += (' ' + ' and '.join(join_cdt_zs) + ' ').split(' ')

		# where conditions
		if self.np.cdts is not None:
			if len(self.np.cdts) > 0:
				z += ' where '
				z_toks.append('where')
				for idx, cond in enumerate(self.np.cdts):
					if idx > 0:
						z += self.np.cdt_linkers[idx - 1]
						z_toks.append(self.np.cdt_linkers[idx - 1])
					z += (' ' + cond.fetch_z(temp_tabname_bucket, typenps, propertynps, next_start_tpos) + ' ')
					z_toks += cond.fetch_z(temp_tabname_bucket, typenps, propertynps, next_start_tpos).split(' ')

		# group bys
		if self.np.group_props is not None:
			if len(self.np.group_props) > 0:
				z += ' group by '
				z_toks += ['group', 'by']
				groupby_propnames = []
				for idx, prop in enumerate(self.np.group_props):
					groupby_propnames.append(prop.z.format(temp_tabname_bucket[prop.table_id]))
				z += (' ' + ' , '.join(groupby_propnames) + ' ')
				z_toks += (' ' + ' , '.join(groupby_propnames) + ' ').split()

		# having conditions
		if self.np.having_cdts is not None and len(self.np.having_cdts) > 0:
			z += ' having '
			z_toks.append('having')
			for hv_idx, item in enumerate(self.np.having_cdts):
				z += item.fetch_z(temp_tabname_bucket, typenps, propertynps, next_start_tpos)
				z_toks += item.fetch_z(temp_tabname_bucket, typenps, propertynps, next_start_tpos).split(' ')
				if hv_idx < len(self.np.having_cdts) - 1:
					z += ', '

		if self.np.orderby_props is not None:
			if len(self.np.orderby_props) > 0:
				z += ' order by '
				z_toks += ['order', 'by']
				orderby_propnames = []
				for idx, prop in enumerate(self.np.orderby_props):
					if prop.dtype == 'star':
						orderby_propnames.append(prop.z)
					else:
						orderby_propnames.append(prop.z.format(temp_tabname_bucket[prop.table_id]))
				z += (' ' + ', '.join(orderby_propnames) + ' ')
				z_toks += (' ' + ', '.join(orderby_propnames) + ' ').split()
				z += ' %s ' % self.np.orderby_order
				z_toks.append(self.np.orderby_order)

		if self.np.limit is not None:
			assert (self.np.limit > 0)
			z += 'limit %d' % self.np.limit
			z_toks += ['limit', '#%d#' % self.np.limit]  # use '#' to wrap the limit number as a 'value'

		# first strip the '#' tokens from z_toks once in order to get executable z
		z_toks_stripped = []
		for item in z_toks:
			if isinstance(item, list):
				raise AssertionError
			if '#' not in item:
				z_toks_stripped.append(item)
			else:
				if not (item[0] == '#' or item[-1] == '#'):
					print(z)
					raise AssertionError
				z_toks_stripped.append(item.strip('#'))
		z = ' '.join(z_toks_stripped)

		# then split the aggregators in order to get the correct z_toks
		z_toks = solve_ztok_aggr(z_toks)
		# use '#' as an indicator for values
		z_toks_novalue = []
		z_toks_stripped = []
		for item in z_toks:
			if isinstance(item, list):
				raise AssertionError
			if '#' not in item:
				z_toks_novalue.append(item)
				z_toks_stripped.append(item)
			else:
				z_toks_novalue.append('value')
				if not (item[0] == '#' or item[-1] == '#'):
					print(z)
					raise AssertionError
				z_toks_stripped.append(item.strip('#'))

		if self.np.np_2 is not None:
			assert self.np.qrynp_2 is not None
			q2z, q2ztoks, q2ztok2novalue = self.np.qrynp_2.process_z(typenps, propertynps, start_tpos=next_start_tpos)
			if self.np.has_union:
				z = '( ' + z + ' ) union ( ' + q2z + ' )'
				z_toks_stripped.append('union')
				z_toks_stripped += q2ztoks
				z_toks_novalue.append('union')
				z_toks_novalue += q2ztok2novalue
			elif self.np.has_except:
				z = '( ' + z + ' ) except ( ' + q2z + ' )'
				z_toks_stripped.append('except')
				z_toks_stripped += q2ztoks
				z_toks_novalue.append('except')
				z_toks_novalue += q2ztok2novalue
			elif self.np.has_intersect:
				z = '( ' + z + ' ) intersect ( ' + q2z + ' )'
				z_toks_stripped.append('intersect')
				z_toks_stripped += q2ztoks
				z_toks_novalue.append('intersect')
				z_toks_novalue += q2ztok2novalue
			else:
				raise AssertionError

		z_toks_fin = []
		z_toks_novalue_fin = []
		for item in z_toks_stripped:
			if len(item) > 0:
				z_toks_fin.append(item)
		for item in z_toks_novalue:
			if len(item) > 0:
				z_toks_novalue_fin.append(item)

		return z, z_toks_fin, z_toks_novalue_fin

	def process_ce_verbose(self, typenps, propertynps, is_recur=False):
		c_english = ''
		if self.np.group_props is not None:
			if len(self.np.group_props) > 0:
				c_english += ' for each value of '
		else:
			self.np.group_props = []
		for idx, prop in enumerate(self.np.group_props):
			c_english += prop.c_english.format('')
			if idx + 2 < len(self.np.group_props):
				c_english += ', '
			elif idx + 2 == len(self.np.group_props):
				c_english += ' and '

		if self.np.having_cdts is not None and len(self.np.having_cdts) > 0:
			c_english += ' having '
			for hv_idx, item in enumerate(self.np.having_cdts):
				c_english += item.c_english
				if hv_idx < len(self.np.having_cdts) - 1:
					c_english += ' , '
		if self.np.group_props is not None:
			if len(self.np.group_props) > 0:
				c_english += ' , '

		if self.np.distinct:
			c_english += 'all different values of '

		# queried_props
		tableid_of_last_prop = None
		for idx, prop in enumerate(self.np.queried_props):
			if isinstance(prop.table_id, list):
				c_english += prop.c_english
			else:
				if prop.table_id == tableid_of_last_prop or len(self.np.table_ids) < 2:
					name = prop.c_english.format('')
				else:
					name = prop.c_english.format(typenps[prop.table_id].c_english + '\'s ')
					tableid_of_last_prop = prop.table_id
				c_english += name
			if idx + 2 < len(self.np.queried_props):
				c_english += ', '
			elif idx + 2 == len(self.np.queried_props):
				c_english += ' and '

		if self.np.main_tids is None:
			self.np.main_tids = []

		# simplify the canonical utterances for join-on conditions when possible in order to help turkers understand
		# and more accurately annotate
		if self.np.join_cdts is not None and len(self.np.join_cdts) > 0 and len(self.np.main_tids) > 0:
			assert len(self.np.table_ids) > 1
			for mid in self.np.main_tids:
				assert mid in self.np.table_ids
			c_english += ' from '
			for mtid_idx, mtid in enumerate(self.np.main_tids):
				c_english += typenps[mtid].c_english
				if mtid_idx < len(self.np.main_tids) - 1:
					c_english += ', '
			c_english += ' and their corresponding '
			other_tids = copy.deepcopy(self.np.table_ids)
			for mtid in self.np.main_tids:
				other_tids.remove(mtid)
			for mtid in self.np.main_tids:
				assert mtid not in other_tids
			for idx, table_id in enumerate(other_tids):
				c_english += typenps[table_id].c_english
				if idx + 2 < len(other_tids):
					c_english += ', '
				elif idx + 2 == len(other_tids):
					c_english += ' and '
		elif len(self.np.table_ids) == 1 and is_recur is True:
			pass
		else:
			c_english += ' from '
			for idx, table_id in enumerate(self.np.table_ids):
				c_english += typenps[table_id].c_english
				if idx + 2 < len(self.np.table_ids):
					c_english += ', '
				elif idx + 2 == len(self.np.table_ids):
					c_english += ' and '

			if self.np.join_cdts is not None:
				if len(self.np.join_cdts) > 0:
					c_english += ' whose '
			else:
				self.np.join_cdts = []
			# all sorts of conditions
			for idx, cond in enumerate(self.np.join_cdts):
				c_english += cond.c_english
				if idx + 2 < len(self.np.join_cdts):
					c_english += ', '
				elif idx + 2 == len(self.np.join_cdts):
					c_english += ' and '

		if self.np.cdts is not None:
			if len(self.np.cdts) > 0:
				if self.np.join_cdts is not None and len(self.np.join_cdts) > 0:
					c_english += ','
				c_english += ' whose '
		else:
			self.np.cdts = []

		for idx, cond in enumerate(self.np.cdts):
			c_english += cond.c_english
			if idx + 1 < len(self.np.cdts):
				c_english += ' ' + self.np.cdt_linkers[idx] + ' '

		if self.np.orderby_props is not None:
			if len(self.np.orderby_props) > 0:
				assert self.np.orderby_order is not None
				if self.np.orderby_order == 'asc':
					_order = 'ascending'
					most = 'smallest'
				elif self.np.orderby_order == 'desc':
					_order = 'descending'
					most = 'largest'
				else:
					raise AssertionError
				if self.np.limit is None or self.np.limit == MAX_RETURN_ENTRIES:
					c_english += ', presented in %s order of ' % _order
				elif self.np.limit == 1:
					c_english += ' with the %s ' % most
				else:
					c_english += ', keep only %d of them with %s ' % (self.np.limit, most)
		else:
			self.np.orderby_props = []
		for idx, item in enumerate(self.np.orderby_props):
			c_english += item.c_english.format('')
			if idx + 1 < len(self.np.orderby_props):
				c_english += ', '

		if self.np.has_union:
			c_english = '( ' + c_english + ' ) combined with ( ' + self.np.qrynp_2.c_english + ' )'
		elif self.np.has_except:
			c_english = '( ' + c_english + ' ) except ( ' + self.np.qrynp_2.c_english + ' )'
		elif self.np.has_intersect:
			c_english = 'those in both ( ' + c_english + ' ) and ( ' + self.np.qrynp_2.c_english + ' )'

		c_english = c_english.replace('%', '')
		return c_english

	def process_ce_step_by_step(self, typenps, propertynps, finalize=False):
		c_english = []
		sent_1 = 'Result {0[0]}: Find '
		if self.np.distinct:
			sent_1 += 'all different values of '

		num_tables = len(self.np.table_ids)

		# queried_props info
		tableid_of_last_prop = None
		need_from_even_single = False
		# if there are no group-by clauses in this query, write down the ultimate version of queried props directly
		if self.np.group_props is None or len(self.np.group_props) == 0:
			for idx, prop in enumerate(self.np.queried_props):
				# if it is a '*' column
				if isinstance(prop.table_id, list):
					sent_1 += prop.c_english
					if prop.aggr != 3:
						need_from_even_single = True
				else:
					if prop.table_id == tableid_of_last_prop or len(self.np.table_ids) < 2:
						name = prop.c_english.format('')
					else:
						name = prop.c_english.format(typenps[prop.table_id].c_english + '\'s ')
						tableid_of_last_prop = prop.table_id
					sent_1 += name
				if idx + 2 < len(self.np.queried_props):
					sent_1 += ', '
				elif idx + 2 == len(self.np.queried_props):
					sent_1 += ' and '
		else:
			# if there are group-by clauses in this query, at first we don't add use aggregators, instead we keep all entries
			for idx, prop in enumerate(self.np.queried_props):
				# if it is a '*' column
				if isinstance(prop.table_id, list):
					sent_1 += STAR_PROP.c_english
					need_from_even_single = True
				else:
					if prop.table_id == tableid_of_last_prop:
						name = propertynps[prop.overall_idx].c_english.format('')
					else:
						name = propertynps[prop.overall_idx].c_english.format(typenps[prop.table_id].c_english + '\'s ')
						tableid_of_last_prop = prop.table_id
					sent_1 += name
				if idx + 2 < len(self.np.queried_props):
					sent_1 += ', '
				elif idx + 2 == len(self.np.queried_props):
					sent_1 += ' and '

		if self.np.main_tids is None:
			self.np.main_tids = []

		# simplify the canonical utterances for join-on conditions when possible in order to help turkers understand
		# and more accurately annotate
		if self.np.join_cdts is not None and len(self.np.join_cdts) > 0 and len(self.np.main_tids) > 0:
			assert num_tables > 1 and len(self.np.table_ids) > 1
			for mid in self.np.main_tids:
				assert mid in self.np.table_ids
			sent_1 += ' from '
			for mtid_idx, mtid in enumerate(self.np.main_tids):
				sent_1 += typenps[mtid].c_english
				if mtid_idx < len(self.np.main_tids) - 1:
					sent_1 += ', '
			sent_1 += ' and their corresponding '
			other_tids = copy.deepcopy(self.np.table_ids)
			for mtid in self.np.main_tids:
				other_tids.remove(mtid)
			for mtid in self.np.main_tids:
				assert mtid not in other_tids
			for idx, table_id in enumerate(other_tids):
				sent_1 += typenps[table_id].c_english
				if idx + 2 < len(other_tids):
					sent_1 += ', '
				elif idx + 2 == len(other_tids):
					sent_1 += ' and '
		else:
			# table info
			if num_tables > 1 or need_from_even_single:
				sent_1 += ' from '
				for idx, table_id in enumerate(self.np.table_ids):
					sent_1 += typenps[table_id].c_english
					if idx + 2 < len(self.np.table_ids):
						sent_1 += ', '
					elif idx + 2 == len(self.np.table_ids):
						sent_1 += ' and '
			# join-on conditions info
			if self.np.join_cdts is not None and len(self.np.join_cdts) > 0:
				sent_1 += ' whose '
			else:
				self.np.join_cdts = []
			for idx, cond in enumerate(self.np.join_cdts):
				sent_1 += cond.c_english
				if idx + 2 < len(self.np.join_cdts):
					sent_1 += ', '
				elif idx + 2 == len(self.np.join_cdts):
					sent_1 += ' and '

		c_english.append(sent_1)

		if self.np.cdts is not None:
			if len(self.np.cdts) > 0:
				sent_2 = 'Result {0[%d]}: From Result {0[%d]}, find those satisfying ' % (
					len(c_english), 0)
				for idx, cond in enumerate(self.np.cdts):
					if isinstance(cond.right, QRYNP):
						cond_right_sent = 'Result {0[%d]}: Find' % len(c_english) + cond.right.c_english
						c_english.append(cond_right_sent)
						sent_2 += cond.left.c_english.format('') + cond.cmper.c_english.format(
							' ( Result {0[%d]} ) ' % (len(c_english)-1))
					else:
						sent_2 += cond.c_english
					if idx + 1 < len(self.np.cdts):
						sent_2 += ' ' + self.np.cdt_linkers[idx] + ' '
				c_english.append(sent_2)
		else:
			self.np.cdts = []

		'''
		# where conditions info
		is_and = None  # whether the condition linker for where conditions is 'and'
		if self.np.cdts is None:
			self.np.cdts = []
		_idx = 0
		while _idx < len(self.np.cdts):
			if _idx + 1 < len(self.np.cdts):
				next_column_id = self.np.cdts[_idx + 1].left.overall_idx
			else:
				next_column_id = None
			# allows for 'and' & 'or' linkers both present in a set of 'where' conditions (though not useful as of
			# SPIDER's scope)
			if is_and is None:
				cur_sent = 'Result {0[%d]}: From Result {0[%d]}, find those satisfying ' % (
					len(c_english), len(c_english) - 1)
			else:
				if is_and:
					cur_sent = 'Result {0[%d]}: From Result {0[%d]}, find those satisfying ' % (
						len(c_english), len(c_english) - 1)
				else:
					cur_sent = 'Result {0[%d]}: Find in addition to Result {0[%d]}, those from Result {0[%d]} satisfying ' % (
						len(c_english), len(c_english) - 1, max(0, len(c_english) - 2))
			cur_sent += self.np.cdts[_idx].c_english
			if next_column_id is not None and next_column_id == self.np.cdts[_idx].left.overall_idx:
				cur_sent += ' ' + self.np.cdt_linkers[_idx]
				next_cond = self.np.cdts[_idx + 1]
				if isinstance(next_cond.right, numpy.ndarray) or isinstance(next_cond.right, list):
					value_ce = []
					for item in next_cond.right:
						value_ce.append(item.c_english)
					utt_next = ' ' + next_cond.cmper.c_english.format(value_ce)
				elif isinstance(next_cond.right, PROPERTYNP):
					utt_next = ' ' + next_cond.cmper.c_english.format(next_cond.right.c_english.format(''))
				elif isinstance(next_cond.right, QRYNP):
					utt_next = ' ' + next_cond.cmper.c_english.format(' ( ' + next_cond.right.c_english + ' ) ')
				else:
					raise AssertionError
				if 'is' in utt_next[:4]:
					utt_next = utt_next[4:]
				cur_sent += utt_next
				_idx += 1
			if _idx < len(self.np.cdts) - 1:
				is_and = (self.np.cdt_linkers[_idx] == 'and')  # is_and value may change from True to False
			c_english.append(cur_sent)
			_idx += 1
		'''

		selected_in_groupby = True
		for prop_1 in self.np.queried_props:
			in_groupby = False
			for prop_2 in self.np.group_props:
				if prop_1.overall_idx == prop_2.overall_idx:
					in_groupby = True
					break
			if not in_groupby:
				selected_in_groupby = False
				break

		# groupby info
		if self.np.group_props is not None:
			if len(self.np.group_props) > 0:
				groupby_sent = 'Result {0[%d]}: From Result {0[%d]}, for each value of ' % (
					len(c_english), len(c_english) - 1)
				for idx, prop in enumerate(self.np.group_props):
					groupby_sent += prop.c_english.format('')
					if idx + 2 < len(self.np.group_props):
						groupby_sent += ', '
					elif idx + 2 == len(self.np.group_props):
						groupby_sent += ' and '
				if self.np.having_cdts is not None and len(self.np.having_cdts) > 0:
					groupby_sent += ' where '
					for item in self.np.having_cdts:
						groupby_sent += (item.c_english + ' , ')
				else:
					groupby_sent += ' , '

				# if there are group-by clauses in query, specify the aggregators of queried props along with the
				# group-by clause itself
				groupby_sent += 'find '

				if selected_in_groupby and self.np.limit is not None and self.np.limit != MAX_RETURN_ENTRIES:
					groupby_sent += 'top %d of ' % self.np.limit

				if self.np.distinct:
					if selected_in_groupby and self.np.limit is not None and self.np.limit != MAX_RETURN_ENTRIES:
						groupby_sent += 'different values of '
					else:
						groupby_sent += 'all different values of '
				tableid_of_last_prop = None
				for idx, prop in enumerate(self.np.queried_props):
					# if it is a '*' column
					if isinstance(prop.table_id, list):
						groupby_sent += prop.c_english
					else:
						if prop.table_id == tableid_of_last_prop:
							name = prop.c_english.format('')
						else:
							name = prop.c_english.format(typenps[prop.table_id].c_english + '\'s ')
							tableid_of_last_prop = prop.table_id
						groupby_sent += name
					if idx + 2 < len(self.np.queried_props):
						groupby_sent += ', '
					elif idx + 2 == len(self.np.queried_props):
						groupby_sent += ' and '

				if selected_in_groupby and self.np.orderby_props is not None and len(self.np.orderby_props) > 0:
					if self.np.orderby_order == 'asc':
						_order = 'ascending'
					elif self.np.orderby_order == 'desc':
						_order = 'descending'
					else:
						raise AssertionError
					groupby_sent += ' in %s order of ' % _order
					for idx, item in enumerate(self.np.orderby_props):
						groupby_sent += item.c_english.format('')
						if idx + 1 < len(self.np.orderby_props):
							groupby_sent += ' then '
				c_english.append(groupby_sent)

		if selected_in_groupby:
			assert self.np.group_props is not None

		if self.np.orderby_props is not None and not selected_in_groupby:
			if len(self.np.orderby_props) > 0:
				assert self.np.orderby_order is not None
				istop1 = None
				if self.np.limit is not None and self.np.limit == 1:
					istop1 = True
					if self.np.orderby_order == 'asc':
						_order = 'least'
					elif self.np.orderby_order == 'desc':
						_order = 'most'
					else:
						raise AssertionError
					orderby_sent = 'Result {0[%d]}: find Result {0[%d]} with the %s ' % (
						len(c_english), len(c_english) - 1, _order)
				else:
					istop1 = False
					if self.np.limit is not None and self.np.limit != MAX_RETURN_ENTRIES:
						limit_ce = 'top %d of ' % self.np.limit
					else:
						limit_ce = ''
					if self.np.orderby_order == 'asc':
						_order = 'ascending'
					elif self.np.orderby_order == 'desc':
						_order = 'descending'
					else:
						raise AssertionError
					orderby_sent = 'Result {0[%d]}: present %sResult {0[%d]} in %s order of ' % (
						len(c_english), limit_ce, len(c_english) - 1, _order)
				for idx, item in enumerate(self.np.orderby_props):
					orderby_sent += item.c_english.format('')
					if idx + 1 < len(self.np.orderby_props):
						if istop1:
							if idx + 2 == len(self.np.orderby_props):
								orderby_sent += ' and '
							else:
								orderby_sent += ', '
						else:
							orderby_sent += ' then '
				c_english.append(orderby_sent)

		if self.np.has_union or self.np.has_except or self.np.has_intersect:
			second_sents = self.np.qrynp_2.c_english_sequence
			len_cur_sents = len(c_english)
			len_second_sents = len(second_sents)
			second_sents = ' ### '.join(second_sents)
			second_sents = second_sents.format([('{0[%d]}' % (i + len(c_english))) for i in range(len_second_sents)])
			second_sents = second_sents.split(' ### ')
			c_english += second_sents
			if self.np.has_union:
				post_clause = 'Result {0[%d]}: return those in Result {0[%d]} as well as those in Result {0[%d]}' \
							  % (len(c_english), len_cur_sents - 1, len_cur_sents + len_second_sents - 1)
			elif self.np.has_except:
				post_clause = 'Result {0[%d]}: return those in Result {0[%d]} except those in Result {0[%d]}' \
							  % (len(c_english), len_cur_sents - 1, len_cur_sents + len_second_sents - 1)
			elif self.np.has_intersect:
				post_clause = 'Result {0[%d]}: return those in both Result {0[%d]} and Result {0[%d]}' \
							  % (len(c_english), len_cur_sents - 1, len_cur_sents + len_second_sents - 1)
			else:
				raise AssertionError
			c_english.append(post_clause)

		# in case of the upper most layer of recursion, finalize the sequence by replacing the '{0[xxx]}' number
		# placeholders with actual numbers 'xxx'
		if finalize:
			total_len = len(c_english)
			c_english = ' ### '.join(c_english)
			c_english = c_english.format([i for i in range(total_len)])
			c_english = c_english.split(' ### ')

		# if sequence is short, there'd be no need to make it a sequence
		if len(c_english) < 3:
			c_english = ['Result 0: Find '+self.process_ce_verbose(typenps, propertynps)]

		for ce_i in range(len(c_english)):
			c_english[ce_i] = c_english[ce_i].replace('%', '')

		return c_english

	def process_cc_verbose(self, typenps, propertynps, is_recur=False):
		c_chinese = ''
		if self.np.group_props is not None:
			if len(self.np.group_props) > 0:
				c_chinese += '对于每一个'
		else:
			self.np.group_props = []

		if self.np.having_cdts is not None and len(self.np.having_cdts) > 0:
			c_chinese += '满足'
			for hv_idx, item in enumerate(self.np.having_cdts):
				c_chinese += item.c_chinese
				if hv_idx < len(self.np.having_cdts) - 1:
					c_chinese += '、'
			c_chinese += '的'

		for idx, prop in enumerate(self.np.group_props):
			c_chinese += prop.c_chinese.format('')
			if idx + 2 < len(self.np.group_props):
				c_chinese += '、'
			elif idx + 2 == len(self.np.group_props):
				c_chinese += '和'
		if len(self.np.group_props) > 0:
			c_chinese += '的取值，'

		if self.np.main_tids is None:
			self.np.main_tids = []

		if self.np.join_cdts is not None and len(self.np.join_cdts) > 0 and len(self.np.main_tids) > 0:
			assert len(self.np.table_ids) > 1
			for mid in self.np.main_tids:
				assert mid in self.np.table_ids
			c_chinese += '从'
			for mtid_idx, mtid in enumerate(self.np.main_tids):
				c_chinese += typenps[mtid].c_chinese
				if mtid_idx < len(self.np.main_tids) - 1:
					c_chinese += '、'
			c_chinese += '以及相应的'
			other_tids = copy.deepcopy(self.np.table_ids)
			for mtid in self.np.main_tids:
				other_tids.remove(mtid)
			for mtid in self.np.main_tids:
				assert mtid not in other_tids
			for idx, table_id in enumerate(other_tids):
				c_chinese += typenps[table_id].c_chinese
				if idx + 2 < len(other_tids):
					c_chinese += '、'
				elif idx + 2 == len(other_tids):
					c_chinese += '和'
			c_chinese += '中，找出'
		elif len(self.np.table_ids) == 1 and is_recur is True:
			c_chinese += '找出'
		else:
			c_chinese += '从'
			for idx, table_id in enumerate(self.np.table_ids):
				c_chinese += typenps[table_id].c_chinese
				if idx + 2 < len(self.np.table_ids):
					c_chinese += '、'
				elif idx + 2 == len(self.np.table_ids):
					c_chinese += '和'
			c_chinese += '中找出，'

			if self.np.join_cdts is not None:
				if len(self.np.join_cdts) > 0:
					c_chinese += '满足'
			else:
				self.np.join_cdts = []
			# all sorts of conditions
			for idx, cond in enumerate(self.np.join_cdts):
				c_chinese += cond.c_chinese
				if idx + 2 <= len(self.np.join_cdts):
					c_chinese += '、'
			if len(self.np.join_cdts) > 0:
				c_chinese += '的，'

		if self.np.cdts is not None:
			if len(self.np.cdts) > 0:
				c_chinese += '满足'
		else:
			self.np.cdts = []
		for idx, cond in enumerate(self.np.cdts):
			c_chinese += cond.c_chinese
			if idx + 1 < len(self.np.cdts):
				if self.np.cdt_linkers[idx] == 'or':
					c_chinese += '或'
				elif self.np.cdt_linkers[idx] == 'and':
					c_chinese += '且'
				else:
					raise AssertionError
		if len(self.np.cdts) > 0:
			c_chinese += '的，'

		# queried_props
		tableid_of_last_prop = None
		for idx, prop in enumerate(self.np.queried_props):
			if isinstance(prop.table_id, list):
				c_chinese += prop.c_chinese
			else:
				if prop.table_id == tableid_of_last_prop or len(self.np.table_ids) < 2:
					name = prop.c_chinese.format('')
				else:
					name = prop.c_chinese.format(typenps[prop.table_id].c_chinese + '的')
					tableid_of_last_prop = prop.table_id
				c_chinese += name
			if idx + 2 < len(self.np.queried_props):
				c_chinese += '、'
			elif idx + 2 == len(self.np.queried_props):
				c_chinese += '和'

		if self.np.distinct:
			c_chinese += '的全部不同取值'

		if self.np.orderby_props is not None:
			if len(self.np.orderby_props) > 0:
				orderby_props_cc = ''
				for idx, item in enumerate(self.np.orderby_props):
					orderby_props_cc += item.c_chinese.format('')
					if idx + 1 < len(self.np.orderby_props):
						orderby_props_cc += '、'
				assert self.np.orderby_order is not None
				if self.np.orderby_order == 'asc':
					_order = '升'
				elif self.np.orderby_order == 'desc':
					_order = '降'
				else:
					raise AssertionError
				if self.np.limit is None or self.np.limit == MAX_RETURN_ENTRIES:
					c_chinese += '，按%s的%s序排列' % (orderby_props_cc, _order)
				else:
					if self.np.orderby_order == 'asc':
						most = '低'
					elif self.np.orderby_order == 'desc':
						most = '高'
					else:
						raise AssertionError
					c_chinese += '，仅选出%s最%s的%d个' % (orderby_props_cc, most, self.np.limit)
		else:
			self.np.orderby_props = []

		if self.np.has_union:
			c_chinese = '返回（' + c_chinese + '）中的全部内容和（' + self.np.qrynp_2.c_chinese + '）中的全部内容'
		elif self.np.has_except:
			c_chinese = '返回（' + c_chinese + '）的内容中，除去（' + self.np.qrynp_2.c_chinese + '）之外的部分'
		elif self.np.has_intersect:
			c_chinese = '返回同时在（' + c_chinese + '）和（' + self.np.qrynp_2.c_chinese + '）中的内容'

		c_chinese = c_chinese.replace('%', '')
		return c_chinese

	def process_cc_step_by_step(self, typenps, propertynps, finalize=False):
		c_chinese = []
		sent_1 = '{0[0]}号结果：'
		num_tables = len(self.np.table_ids)

		need_from_even_single = False
		for idx, prop in enumerate(self.np.queried_props):
			# if it is a '*' column
			if isinstance(prop.table_id, list):
				if prop.aggr != 3:
					need_from_even_single = True

		if self.np.main_tids is None:
			self.np.main_tids = []

		# simplify the canonical utterances for join-on conditions when possible in order to help turkers understand
		# and more accurately annotate
		if self.np.join_cdts is not None and len(self.np.join_cdts) > 0 and len(self.np.main_tids) > 0:
			assert num_tables > 1 and len(self.np.table_ids) > 1
			for mid in self.np.main_tids:
				assert mid in self.np.table_ids
			sent_1 += '从'
			for mtid_idx, mtid in enumerate(self.np.main_tids):
				sent_1 += typenps[mtid].c_chinese
				if mtid_idx < len(self.np.main_tids) - 1:
					sent_1 += '、'
			sent_1 += '以及相应的'
			other_tids = copy.deepcopy(self.np.table_ids)
			for mtid in self.np.main_tids:
				other_tids.remove(mtid)
			for mtid in self.np.main_tids:
				assert mtid not in other_tids
			for idx, table_id in enumerate(other_tids):
				sent_1 += typenps[table_id].c_chinese
				if idx + 2 < len(other_tids):
					sent_1 += '、'
				elif idx + 2 == len(other_tids):
					sent_1 += '和'
			sent_1 += '中，找出'
		else:
			# table info
			if num_tables > 1 or need_from_even_single:
				sent_1 += '从'
				for idx, table_id in enumerate(self.np.table_ids):
					sent_1 += typenps[table_id].c_chinese
					if idx + 2 < len(self.np.table_ids):
						sent_1 += '、'
					elif idx + 2 == len(self.np.table_ids):
						sent_1 += '和'
				sent_1 += '中，找出'
				# join-on conditions info
				if self.np.join_cdts is not None and len(self.np.join_cdts) > 0:
					sent_1 += '符合'
				else:
					self.np.join_cdts = []

				for idx, cond in enumerate(self.np.join_cdts):
					sent_1 += cond.c_chinese
					if idx + 2 < len(self.np.join_cdts):
						sent_1 += '、'
					elif idx + 2 == len(self.np.join_cdts):
						sent_1 += '以及'
				if len(self.np.join_cdts) > 0:
					sent_1 += '的，'
			else:
				sent_1 += '找出'

		if self.np.cdts is not None and len(self.np.cdts) == 1:
			sent_1 += '满足'
			sent_1 += self.np.cdts[0].c_chinese
			sent_1 += '的'

		if self.np.distinct:
			sent_1 += '全部不同的'

		# queried_props info
		tableid_of_last_prop = None
		# if there are no group-by clauses in this query, write down the ultimate version of queried props directly
		if self.np.group_props is None or len(self.np.group_props) == 0:
			for idx, prop in enumerate(self.np.queried_props):
				# if it is a '*' column
				if isinstance(prop.table_id, list):
					sent_1 += prop.c_chinese
				else:
					if prop.table_id == tableid_of_last_prop:
						name = prop.c_chinese.format('')
					else:
						name = prop.c_chinese.format(typenps[prop.table_id].c_chinese + '的')
						tableid_of_last_prop = prop.table_id
					sent_1 += name
				if idx + 2 < len(self.np.queried_props):
					sent_1 += '、'
				elif idx + 2 == len(self.np.queried_props):
					sent_1 += '和'
		else:
			# if there are group-by clauses in this query, at first we don't add use aggregators, instead we keep all entries
			for idx, prop in enumerate(self.np.queried_props):
				# if it is a '*' column
				if isinstance(prop.table_id, list):
					sent_1 += STAR_PROP.c_chinese
					need_from_even_single = True
				else:
					if prop.table_id == tableid_of_last_prop:
						name = propertynps[prop.overall_idx].c_chinese.format('')
					else:
						name = propertynps[prop.overall_idx].c_chinese.format(typenps[prop.table_id].c_chinese + '的')
						tableid_of_last_prop = prop.table_id
					sent_1 += name
				if idx + 2 < len(self.np.queried_props):
					sent_1 += '、'
				elif idx + 2 == len(self.np.queried_props):
					sent_1 += '和'
		c_chinese.append(sent_1)

		sent_2 = ''
		if self.np.cdts is not None:
			if len(self.np.cdts) > 0:
				sent_2 += '{0[%d]}号结果：从{0[%d]}号结果中，找出满足' % (len(c_chinese), 0)
		else:
			self.np.cdts = []
		for idx, cond in enumerate(self.np.cdts):

			if isinstance(cond.right, QRYNP):
				cond_right_sent = '{0[%d]}号结果：' % len(c_chinese) + cond.right.c_chinese
				c_chinese.append(cond_right_sent)
				sent_2 += cond.left.c_chinese.format('') + cond.cmper.c_chinese.format(
					'（{0[%d]}号结果）' % (len(c_chinese) - 1))
			else:
				sent_2 += cond.c_english
			sent_2 += cond.c_chinese
			if idx + 1 < len(self.np.cdts):
				if self.np.cdt_linkers[idx] == 'or':
					sent_2 += '或'
				elif self.np.cdt_linkers[idx] == 'and':
					sent_2 += '且'
				else:
					raise AssertionError
		if len(self.np.cdts) > 0:
			sent_2 += '的，'
		if len(sent_2) > 0:
			c_chinese.append(sent_2)

		'''
		# where conditions inf
		is_and = None  # whether the condition linker for where conditions is 'and'
		if self.np.cdts is None:
			self.np.cdts = []
		linkers_trans = {'and': '且', 'or': '或'}  # translates cdt_linkers 'and' & 'or' into Chinese words
		# the index of condition to work on, set to 1 to skip if len=1 # (in which cases condition is merged with sent_1)
		_idx = 0 if len(self.np.cdts)!=1 else 1
		while _idx < len(self.np.cdts):
			if _idx + 1 < len(self.np.cdts):
				next_column_id = self.np.cdts[
					_idx + 1].left.overall_idx  # if not the last one, check the column for next cdt
			else:
				next_column_id = None
			# allows for 'and' & 'or' linkers both present in a set of 'where' conditions (though not useful as of
			# SPIDER's scope)
			if is_and is None:
				cur_sent = '{0[%d]}号结果：从{0[%d]}号结果中，找出满足' % (
					len(c_chinese), len(c_chinese) - 1)
			else:
				if is_and:
					cur_sent = '{0[%d]}号结果：从{0[%d]}号结果中，找出满足' % (
						len(c_chinese), len(c_chinese) - 1)
				else:
					cur_sent = '{0[%d]}号结果: 在{0[%d]}号结果之余，找出{0[%d]}号结果中满足' % (
						len(c_chinese), len(c_chinese) - 1, max(0, len(c_chinese) - 2))
			cur_sent += self.np.cdts[_idx].c_chinese
			if next_column_id is not None and next_column_id == self.np.cdts[_idx].left.overall_idx:
				cur_sent += linkers_trans[self.np.cdt_linkers[_idx]]
				next_cond = self.np.cdts[_idx + 1]
				if isinstance(next_cond.right, numpy.ndarray) or isinstance(next_cond.right, list):
					value_cc = []
					for item in next_cond.right:
						value_cc.append(item.c_chinese)
					utt_next = next_cond.cmper.c_chinese.format(value_cc)
				elif isinstance(next_cond.right, PROPERTYNP):
					utt_next = next_cond.cmper.c_chinese.format(next_cond.right.c_chinese.format(''))
				elif isinstance(next_cond.right, QRYNP):
					utt_next = next_cond.cmper.c_chinese.format(' ( ' + next_cond.right.c_chinese + ' ) ')
				else:
					raise AssertionError
				cur_sent += utt_next
				_idx += 1
			cur_sent += '的部分'
			if _idx < len(self.np.cdts) - 1:
				is_and = (self.np.cdt_linkers[_idx] == 'and')  # is_and value may change from True to False
			c_chinese.append(cur_sent)
			_idx += 1
		'''

		# check if every selected property can be found in 'group-by' property list
		selected_in_groupby = True
		for prop_1 in self.np.queried_props:
			in_groupby = False
			for prop_2 in self.np.group_props:
				if prop_1.overall_idx == prop_2.overall_idx:
					in_groupby = True
					break
			if not in_groupby:
				selected_in_groupby = False
				break

		# groupby info
		if self.np.group_props is not None and len(self.np.group_props) > 0:
			groupby_sent = '{0[%d]}号结果：从{0[%d]}号结果中，在每组' % (
				len(c_chinese), len(c_chinese) - 1)

			if self.np.having_cdts is not None and len(self.np.having_cdts) > 0:
				groupby_sent += '满足'
				for hv_idx, item in enumerate(self.np.having_cdts):
					groupby_sent += item.c_chinese
					if hv_idx + 1 < len(self.np.having_cdts):
						groupby_sent += '、'
				groupby_sent += '的'

			for idx, prop in enumerate(self.np.group_props):
				groupby_sent += prop.c_chinese.format('')
				if idx + 2 < len(self.np.group_props):
					groupby_sent += '、'
				elif idx + 2 == len(self.np.group_props):
					groupby_sent += '和'

			# if there are group-by clauses in query, specify the aggregators of queried props along with the
			# group-by clause itself
			groupby_sent += '下，'

			if selected_in_groupby and self.np.limit is not None and self.np.limit != MAX_RETURN_ENTRIES:
				groupby_sent += '找出具有'
				if self.np.orderby_order == 'asc':
					groupby_sent += '最小'
				elif self.np.orderby_order == 'desc':
					groupby_sent += '最大'
				else:
					raise AssertionError
				for ob_idx, item in enumerate(self.np.orderby_props):
					groupby_sent += item.c_chinese.format('')
					if ob_idx + 1 < len(self.np.orderby_props):
						groupby_sent += '、'
				groupby_sent += '的'
				if self.np.limit != 1:
					groupby_sent += '前%d个' % self.np.limit

			elif selected_in_groupby and self.np.orderby_props is not None and len(self.np.orderby_props) > 0:
				groupby_sent += '按照'
				for ob_idx, item in enumerate(self.np.orderby_props):
					groupby_sent += item.c_chinese.format('')
					if ob_idx + 1 < len(self.np.orderby_props):
						groupby_sent += '、'
				groupby_sent += '的'
				if self.np.orderby_order == 'asc':
					groupby_sent += '升序'
				elif self.np.orderby_order == 'desc':
					groupby_sent += '降序'
				else:
					raise AssertionError
				groupby_sent += '找出'
			else:
				groupby_sent += '找出'

			tableid_of_last_prop = None
			for idx, prop in enumerate(self.np.queried_props):
				# if it is a '*' column
				if isinstance(prop.table_id, list):
					groupby_sent += prop.c_chinese
				else:
					if prop.table_id == tableid_of_last_prop:
						name = prop.c_chinese.format('')
					else:
						name = prop.c_chinese.format(typenps[prop.table_id].c_chinese + '的')
						tableid_of_last_prop = prop.table_id
					groupby_sent += name
				if idx + 2 < len(self.np.queried_props):
					groupby_sent += '、'
				elif idx + 2 == len(self.np.queried_props):
					groupby_sent += '和'

			if self.np.distinct:
				groupby_sent += '，找出其所有的不同取值'
			c_chinese.append(groupby_sent)

		if selected_in_groupby:
			assert self.np.group_props is not None

		if self.np.orderby_props is not None and len(self.np.orderby_props) > 0 and not selected_in_groupby:
			assert self.np.orderby_order is not None
			if self.np.limit is not None and self.np.limit == 1:
				istop1 = True
				if self.np.orderby_order == 'asc':
					_order = '最小的'
				elif self.np.orderby_order == 'desc':
					_order = '最大的'
				else:
					raise AssertionError
				orderby_sent = '{0[%d]}号结果：找出{0[%d]}号结果中' % (
					len(c_chinese), len(c_chinese) - 1)

			else:
				istop1 = False
				if self.np.limit is not None and self.np.limit != MAX_RETURN_ENTRIES:
					limit_cc = '选出前%d个条目' % self.np.limit
				else:
					limit_cc = '排列'
				if self.np.orderby_order == 'asc':
					_order = '升序'
				elif self.np.orderby_order == 'desc':
					_order = '降序'
				else:
					raise AssertionError
				orderby_sent = '{0[%d]}号结果: 将{0[%d]}号结果按' % (
					len(c_chinese), len(c_chinese) - 1)

			for idx, item in enumerate(self.np.orderby_props):
				orderby_sent += item.c_chinese.format('')
				if idx + 1 < len(self.np.orderby_props):
					orderby_sent += '、'

			if istop1:
				orderby_sent += _order
			else:
				orderby_sent += '的%s%s' % (_order, limit_cc)
			c_chinese.append(orderby_sent)

		if self.np.has_union or self.np.has_except or self.np.has_intersect:
			second_sents = self.np.qrynp_2.c_chinese_sequence
			len_cur_sents = len(c_chinese)
			len_second_sents = len(second_sents)
			second_sents = ' ### '.join(second_sents)
			second_sents = second_sents.format([('{0[%d]}' % (i + len(c_chinese))) for i in range(len_second_sents)])
			second_sents = second_sents.split(' ### ')
			c_chinese += second_sents
			if self.np.has_union:
				post_clause = '{0[%d]}号结果：返回{0[%d]}号结果中的全部内容和{0[%d]}号结果中的全部内容' \
							  % (len(c_chinese), len_cur_sents - 1, len_cur_sents + len_second_sents - 1)
				assert not self.np.has_except and not self.np.has_intersect
			elif self.np.has_except:
				post_clause = '{0[%d]}号结果：返回{0[%d]}号结果中不被 {0[%d]}号结果包含的部分' \
							  % (len(c_chinese), len_cur_sents - 1, len_cur_sents + len_second_sents - 1)
				assert not self.np.has_intersect
			elif self.np.has_intersect:
				post_clause = '{0[%d]}号结果：返回所有同时在{0[%d]}号结果和{0[%d]}号结果中的部分' \
							  % (len(c_chinese), len_cur_sents - 1, len_cur_sents + len_second_sents - 1)
			else:
				raise AssertionError
			c_chinese.append(post_clause)

		# in case of the upper most layer of recursion, finalize the sequence by replacing the '{0[xxx]}' number
		# placeholders with actual numbers 'xxx'
		if finalize:
			total_len = len(c_chinese)
			c_chinese = ' ### '.join(c_chinese)
			c_chinese = c_chinese.format([i for i in range(total_len)])
			c_chinese = c_chinese.split(' ### ')

		# if sequence is short, there'd be no need to make it a sequence
		if len(c_chinese) < 3:
			c_chinese = ['0号结果：'+self.process_cc_verbose(typenps, propertynps)]
		for cc_i in range(len(c_chinese)):
			c_chinese[cc_i] = c_chinese[cc_i].replace('%', '')
		return c_chinese


STAR_PROP = PROPERTYNP('everything', '全部信息', '*', 'star', overall_idx=-1, values=[], meta_idx=-1)
