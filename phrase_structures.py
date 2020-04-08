import copy
import random
from utils import *


# built-in functions
# THE RESULTS OF DATA AGGREGATION SHOULD BE VALUENP?
# data aggregation
def _avg(x):
	assert (isinstance(x, PROPERTYNP))
	#assert (x.dtype in CALCULABLE_DTYPES)
	# the property to be averaged must be single
	c_english = 'the average of %s' % x.c_english
	c_chinese = '%s的平均值' % x.c_chinese
	z = 'AVG(%s)' % x.z
	z4toks = 'AVG ( %s ) ' % x.z
	return c_english, c_chinese, z, z4toks, False


def _min(x):
	assert (isinstance(x, PROPERTYNP))
	#assert (x.dtype in SUBTRACTABLE_DTYPES)
	c_english = 'the smallest %s' % x.c_english
	c_chinese = '%s的最小值' % x.c_chinese
	z = 'MIN(%s)' % x.z
	z4toks = 'MIN ( %s )' % x.z
	return c_english, c_chinese, z, z4toks, False


def _max(x):
	assert (isinstance(x, PROPERTYNP))
	#assert (x.dtype in SUBTRACTABLE_DTYPES)
	c_english = 'the largest %s' % x.c_english
	c_chinese = '%s的最大值' % x.c_chinese
	z = 'MAX(%s)' % x.z
	z4toks = 'MAX ( %s )' % x.z
	return c_english, c_chinese, z, z4toks, False


def _sum(x):
	assert (isinstance(x, PROPERTYNP))
	#assert (x.dtype in CALCULABLE_DTYPES)
	c_english = 'the sum of %s' % x.c_english
	c_chinese = '%s的总和' % x.c_chinese
	z = 'SUM(%s)' % x.z
	z4toks = 'SUM ( %s )' % x.z
	return c_english, c_chinese, z, z4toks, False


def _uniquecount(x):
	assert (isinstance(x, PROPERTYNP))

	# the count must be of integer type, specifically ordinal integer type
	rho = random.random()
	if rho < 0.4 and x.dtype != 'star':
		distinct = True
		c_english = 'the number of different values of %s' % x.c_english
		c_chinese = '%s不同取值的数量' % x.c_chinese
		z = 'COUNT(distinct %s)' % x.z
		z4toks = 'COUNT ( distinct %s )' % x.z
	else:
		distinct = False
		if x.dtype == 'star':
			c_english = 'the number of entries'
			c_chinese = '数量'
		else:
			c_english = 'the number of %s' % x.c_english
			c_chinese = '%s的数量' % x.c_chinese
		z = 'COUNT(%s)' % x.z
		z4toks = 'COUNT ( %s )' % x.z
	return c_english, c_chinese, z, z4toks, distinct


# for use in converting an SQL entry into a 'NP' class object
def _count_uniqueness_specified(x, distinct):
	assert (isinstance(x, PROPERTYNP))
	# the count must be of integer type, specifically ordinal integer type
	if distinct and x.dtype != 'star':
		c_english = 'the number of different values of %s' % x.c_english
		c_chinese = '%s不同取值的数量' % x.c_chinese
		z = 'COUNT( distinct %s )' % x.z
		z4toks = 'COUNT ( distinct %s )' % x.z
	else:
		if x.dtype == 'star':
			c_english = 'the number of entries'
			c_chinese = '数量'
		else:
			c_english = 'the number of %s' % x.c_english
			c_chinese = '%s的数量' % x.c_chinese
		z = 'COUNT( %s )' % x.z
		z4toks = 'COUNT ( %s )' % x.z
	return c_english, c_chinese, z, z4toks, distinct


def _repeat(x):
	return x.c_english, x.c_chinese, x.z, x.z, False


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
	def __init__(self, c_english, c_chinese, z, dtype, table_id=None, overall_idx=None, values=None,
				 aggr=0, from_sum=False, from_cnt=False, is_unique=False, meta_idx=None):
		BASENP.__init__(self, c_english, c_chinese, z)
		self.z4toks = self.z
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
			self.c_english, self.c_chinese, self.z, self.z4toks, self.distinct = function(self, distinct)
		else:
			self.c_english, self.c_chinese, self.z, self.z4toks, self.distinct = AGGREGATION_FUNCTIONS[self.aggr](self)
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
		else:
			# 'mode' must be among 'mid', 'head' and 'tail'
			raise AssertionError


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

	def fetch_z(self, temp_tabname_bucket):
		if isinstance(self.right, PROPERTYNP):
			res = self.left.z.format(temp_tabname_bucket[self.left.table_id]) + self.cmper.z.format(
				self.right.z.format(temp_tabname_bucket[self.right.table_id]))
		else:
			# in cv conditions, there might be cases where left column is star, then there need be no formatting
			if self.left.dtype == 'star':
				res = self.z
			else:
				res = self.z.format(temp_tabname_bucket[self.left.table_id])
		return res


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
	#	main_tid:		int
	def __init__(self, prev_np=None, queried_props=None, table_ids=None, join_cdts=None,
				 cdts=None, cdt_linkers=None, group_props=None, having_cdts=None,
				 orderby_props=None, orderby_order=None, limit=None, np_2=None, qrynp_2=None, has_union=None,
				 has_except=None, has_intersect=None, distinct=False, main_tid=None):
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
			self.main_tid = prev_np.main_tid
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
			self.main_tid = main_tid
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


STAR_PROP = PROPERTYNP('everything', '全部信息', '*', 'star', overall_idx=-1, values=[], meta_idx=-1)
